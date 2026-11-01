import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { after, before, test } from "node:test";
import { jetstream, jetstreamManager, type JetStreamManager } from "@nats-io/jetstream";
import {
  DeltaV1,
  ReserveFundsReplyV1,
  ReserveFundsRequestV1,
  RpcSubject,
  SettleReservationReplyV1,
  SettleReservationRequestV1,
} from "@famashines/ai-systems-contracts";
import { loadConfig } from "@famashines/ai-systems-kit/config";
import { connectNats, type Nats } from "@famashines/ai-systems-kit/messaging";
import { connectMongo, type Mongo } from "@famashines/ai-systems-kit/mongo";
import { createLogger } from "@famashines/ai-systems-kit/observability";
import {
  createStreams,
  startMongo,
  startNats,
  waitFor,
  type Started,
} from "@famashines/ai-systems-kit/testing";
import { envSchema, start } from "../../src/main.ts";
import {
  errorReply,
  startMockOpenRouter,
  streamOf,
  type MockOpenRouter,
} from "../mock-openrouter.ts";

const aiProxyGrants = [
  "llm.>",
  "delta.>",
  "rpc.budget.reserve",
  "rpc.budget.settle",
  "dlq.ai-proxy.>",
  "$JS.API.INFO",
  "$JS.API.STREAM.INFO.LLM",
  "$JS.API.STREAM.INFO.ORCHESTRATION",
  "$JS.API.CONSUMER.CREATE.ORCHESTRATION.ai-proxy.orchestration.CallRequested",
  "$JS.API.CONSUMER.INFO.ORCHESTRATION.ai-proxy",
  "$JS.API.CONSUMER.MSG.NEXT.ORCHESTRATION.ai-proxy",
  "$JS.ACK.ORCHESTRATION.ai-proxy.>",
  "dlq.ai-proxy-cancel.>",
  "$JS.API.CONSUMER.CREATE.ORCHESTRATION.ai-proxy-cancel.orchestration.CallCancelRequested",
  "$JS.API.CONSUMER.INFO.ORCHESTRATION.ai-proxy-cancel",
  "$JS.API.CONSUMER.MSG.NEXT.ORCHESTRATION.ai-proxy-cancel",
  "$JS.ACK.ORCHESTRATION.ai-proxy-cancel.>",
];

const budgetGrants = ["budget.>", "dlq.budget.>", "$JS.API.INFO", "$JS.API.STREAM.INFO.BUDGET"];

const socketGatewayGrants = [
  "$JS.API.INFO",
  "$JS.API.STREAM.INFO.VIEWS",
  "$JS.API.CONSUMER.CREATE.VIEWS",
  "$JS.API.CONSUMER.CREATE.VIEWS.>",
  "$JS.API.CONSUMER.INFO.VIEWS.*",
  "$JS.API.CONSUMER.DELETE.VIEWS.*",
  "$JS.API.CONSUMER.MSG.NEXT.VIEWS.*",
];

const adminGrants = [
  "$JS.API.>",
  "$JS.ACK.>",
  "orchestration.>",
  "conversation.>",
  "llm.>",
  "budget.>",
  "agents.>",
  "prompts.>",
  "dlq.>",
];

const list = (subjects: string[]): string => subjects.map((s) => `"${s}"`).join(", ");

const natsConfig = `
jetstream { store_dir: /data }
accounts {
  AISYSTEMS {
    jetstream { max_file: 1073741824, max_memory: 0 }
    users = [
      {
        user: ai-systems-admin, password: admin
        permissions {
          publish { allow: [${list(adminGrants)}] }
          subscribe { allow: ["_INBOX_admin.>"] }
        }
      }
      {
        user: ai-systems-ai-proxy, password: ai-proxy
        permissions {
          publish { allow: [${list(aiProxyGrants)}] }
          subscribe { allow: ["_INBOX_ai-proxy.>"] }
        }
      }
      {
        user: ai-systems-budget, password: budget
        permissions {
          publish { allow: [${list(budgetGrants)}] }
          subscribe { allow: ["_INBOX_budget.>", "rpc.budget.reserve", "rpc.budget.settle"] }
          allow_responses: true
        }
      }
      {
        user: ai-systems-socket-gateway, password: socket-gateway
        permissions {
          publish { allow: [${list(socketGatewayGrants)}] }
          subscribe { allow: ["_INBOX_socket-gateway.>", "delta.>"] }
        }
      }
    ]
  }
}
`;

const logger = createLogger({ service: "test", release: "test", level: "silent" });
const testKey = "integration-test-key";
let mongoContainer: Started;
let natsContainer: Started;
let mongo: Mongo;
let admin: Nats;
let budget: Nats;
let gateway: Nats;
let manager: JetStreamManager;
let mock: MockOpenRouter;
let running: Awaited<ReturnType<typeof start>> | undefined;
let base = "";
const reserves: ReserveFundsRequestV1[] = [];
const settles: SettleReservationRequestV1[] = [];
const deltas: { subject: string; delta: DeltaV1 }[] = [];
const refused = new Set<string>();

before(async () => {
  [mongoContainer, natsContainer, mock] = await Promise.all([
    startMongo(),
    startNats({ config: natsConfig }),
    startMockOpenRouter(),
  ]);
  mongo = await connectMongo({ url: mongoContainer.url, database: "ai_proxy" });
  const nats = (user: string, pass: string, service: string) =>
    connectNats({ servers: natsContainer.url, user, pass, service, logger });
  admin = await nats("ai-systems-admin", "admin", "admin");
  budget = await nats("ai-systems-budget", "budget", "budget");
  gateway = await nats("ai-systems-socket-gateway", "socket-gateway", "socket-gateway");
  manager = await jetstreamManager(admin.connection);
  await createStreams(admin.connection, [
    { name: "ORCHESTRATION", subjects: ["orchestration.>"] },
    { name: "LLM", subjects: ["llm.>"] },
    { name: "DLQ", subjects: ["dlq.>"] },
  ]);
  const allow = { allowReplyPrefixes: ["_INBOX_orchestration.", "_INBOX_ai-proxy."] };
  budget.serve(
    RpcSubject.budgetReserve,
    ReserveFundsRequestV1,
    ReserveFundsReplyV1,
    (input): ReserveFundsReplyV1 => {
      reserves.push(input);
      return refused.has(input.taskId)
        ? {
            ok: false,
            reservationId: input.reservationId,
            scope: { kind: "month", key: "2026-10" },
            reason: "Monthly cap reached: 20.00 of 20.00 USD",
          }
        : { ok: true, reservationId: input.reservationId, expiresAt: new Date().toISOString() };
    },
    allow,
  );
  budget.serve(
    RpcSubject.budgetSettle,
    SettleReservationRequestV1,
    SettleReservationReplyV1,
    (input) => {
      settles.push(input);
      return { state: "settled" as const, ...input };
    },
    allow,
  );
  gateway.connection.subscribe("delta.>", {
    callback: (error, message) => {
      if (!error) deltas.push({ subject: message.subject, delta: DeltaV1.parse(message.json()) });
    },
  });
  const config = loadConfig(envSchema, {
    MONGODB_URI: mongoContainer.url,
    NATS_SERVERS: natsContainer.url,
    NATS_USER: "ai-systems-ai-proxy",
    NATS_PASSWORD: "ai-proxy",
    OPENROUTER_API_KEY: testKey,
    OPENROUTER_BASE_URL: mock.url,
    LOG_LEVEL: "silent",
  });
  running = await start({ ...config, PORT: 0 }, { drainDelayMs: 0 });
  base = `http://127.0.0.1:${String(running.address.port)}`;
  await waitFor(async () => (await fetch(`${base}/ready`)).ok);
});

after(async () => {
  if (running) assert.equal(await running.app.close(), true);
  await Promise.all([admin.close(), budget.close(), gateway.close(), mongo.close(), mock.close()]);
  await Promise.all([mongoContainer.stop(), natsContainer.stop()]);
});

const envelope = (
  payload: Record<string, unknown>,
  taskId: string,
  type = "orchestration.CallRequested",
) => ({
  eventId: randomUUID(),
  type,
  schemaVersion: 1,
  streamId: `task-${taskId}`,
  version: 2,
  occurredAt: new Date().toISOString(),
  correlationId: taskId,
  causationId: randomUUID(),
  actor: { kind: "system", id: "orchestration" },
  payload,
});

const publish = async (event: { eventId: string; type: string }, msgId = event.eventId) => {
  await jetstream(admin.connection).publish(event.type, JSON.stringify(event), { msgID: msgId });
};

const cancelOf = (taskId: string, callId: string) =>
  envelope({ taskId, callId }, taskId, "orchestration.CallCancelRequested");

const callRequest = (models: string[], extra: Record<string, unknown> = {}) => {
  const taskId = randomUUID();
  const callId = randomUUID();
  const event = envelope(
    {
      taskId,
      callId,
      agentId: randomUUID(),
      agentVersion: 1,
      messages: [{ role: "user", content: "Say hello." }],
      models,
      maxTokens: 50,
      maxDurationMs: 30_000,
      ...extra,
    },
    taskId,
  );
  return { taskId, callId, event };
};

const events = () =>
  mongo.db.collection<{
    _id: string;
    streamId: string;
    type: string;
    payload: Record<string, unknown>;
    publishedAt: Date | null;
  }>("events");

const ended = (callId: string, count: number) =>
  waitFor(async () => {
    const stored = await events()
      .find({ streamId: `call-${callId}` })
      .toArray();
    return stored.length === count && stored.every((event) => event.publishedAt !== null);
  });

const typesOf = async (callId: string) =>
  (
    await events()
      .find({ streamId: `call-${callId}` }, { sort: { version: 1 } })
      .toArray()
  ).map((event) => event.type);

const llmSubjects = async (eventIds: string[]) => {
  const { state } = await manager.streams.info("LLM");
  const found: string[] = [];
  for (let seq = state.first_seq; seq <= state.last_seq && state.messages > 0; seq += 1) {
    const message = await manager.streams.getMessage("LLM", { seq });
    if (message && eventIds.includes(message.header.get("Nats-Msg-Id"))) {
      found.push(message.subject);
    }
  }
  return found;
};

test("a call streams deltas, books the cost and publishes the llm events", async () => {
  mock.reply("mock/small", streamOf("mock/small", ["Hello", " there"]));
  const { taskId, callId, event } = callRequest(["mock/small"]);
  await publish(event);
  await ended(callId, 2);
  assert.deepEqual(await typesOf(callId), ["llm.CallStarted", "llm.CallCompleted"]);
  const stored = await events()
    .find({ streamId: `call-${callId}` }, { sort: { version: 1 } })
    .toArray();
  assert.deepEqual(await llmSubjects(stored.map((item) => item._id)), [
    "llm.CallStarted",
    "llm.CallCompleted",
  ]);
  const completed = stored[1]?.payload ?? {};
  assert.equal(completed.text, "Hello there");
  assert.equal(completed.costMicroUsd, 42);
  const reserve = reserves.find((item) => item.taskId === taskId);
  assert.ok(reserve);
  assert.equal(stored[0]?.payload.reservationId, reserve.reservationId);
  assert.deepEqual(
    settles.filter((item) => item.reservationId === reserve.reservationId),
    [{ reservationId: reserve.reservationId, actualMicroUsd: 42 }],
  );
  await waitFor(() => deltas.filter((item) => item.subject.endsWith(callId)).length === 2);
  assert.deepEqual(
    deltas.filter((item) => item.subject === `delta.${taskId}.${callId}`).map((item) => item.delta),
    [
      { attempt: 1, i: 0, text: "Hello" },
      { attempt: 1, i: 1, text: " there" },
    ],
  );
  const body = await mongo.db
    .collection<{ _id: string }>("call_bodies")
    .findOne({ _id: completed.bodyId as string });
  assert.ok(body);
  assert.equal(JSON.stringify(body).includes(testKey), false);
  assert.equal(JSON.stringify(body).toLowerCase().includes("authorization"), false);
  const attempt = await mongo.db
    .collection<{ _id: string; heartbeatAt?: Date }>("attempts")
    .findOne({ _id: `${callId}:1` });
  assert.ok(attempt?.heartbeatAt instanceof Date);
  const received = mock.received.find((item) => item.body.model === "mock/small");
  assert.equal(received?.headers.authorization, `Bearer ${testKey}`);
});

test("a rate limit goes to the next model with a new reservation", async () => {
  mock.reply("mock/large", errorReply(429, "rate_limit_exceeded"));
  mock.reply("mock/free:free", streamOf("mock/free:free", ["Free answer"]));
  const { taskId, callId, event } = callRequest(["mock/large", "mock/free:free"]);
  await publish(event);
  await ended(callId, 4);
  assert.deepEqual(await typesOf(callId), [
    "llm.CallStarted",
    "llm.CallFailed",
    "llm.CallStarted",
    "llm.CallCompleted",
  ]);
  const stored = await events()
    .find({ streamId: `call-${callId}` }, { sort: { version: 1 } })
    .toArray();
  assert.equal("callCostMicroUsd" in (stored[1]?.payload ?? {}), false);
  assert.deepEqual(
    [
      stored[3]?.payload.callInputTokens,
      stored[3]?.payload.callOutputTokens,
      stored[3]?.payload.callCostMicroUsd,
    ],
    [10, 1, 42],
  );
  const mine = reserves.filter((item) => item.taskId === taskId);
  assert.equal(mine.length, 2);
  assert.notEqual(mine[0]?.reservationId, mine[1]?.reservationId);
  assert.ok((mine[0]?.amountMicroUsd ?? 0) > 0);
  assert.equal(mine[1]?.amountMicroUsd, 0);
});

test("a budget refusal stops the call before the provider", async () => {
  const { taskId, callId, event } = callRequest(["mock/small"]);
  refused.add(taskId);
  const before = mock.received.length;
  await publish(event);
  await ended(callId, 1);
  const [failed] = await events()
    .find({ streamId: `call-${callId}` })
    .toArray();
  assert.ok(failed);
  assert.equal(failed.type, "llm.CallFailed");
  assert.equal(failed.payload.errorClass, "budget_refused");
  assert.equal(failed.payload.reason, "Monthly cap reached: 20.00 of 20.00 USD");
  assert.equal(mock.received.length, before);
});

test("a redelivered CallRequested does not call the provider again", async () => {
  const { callId, event } = callRequest(["mock/small"]);
  await publish(event);
  await ended(callId, 2);
  const calls = mock.received.length;
  await publish(event, randomUUID());
  const marker = callRequest(["mock/small"]);
  await publish(marker.event);
  await ended(marker.callId, 2);
  assert.equal(mock.received.length, calls + 1);
  assert.equal((await typesOf(callId)).length, 2);
});

test("the replay API gives the llm events", async () => {
  const response = await fetch(`${base}/internal/events?limit=1000`);
  const { events: all } = (await response.json()) as { events: { type: string }[] };
  assert.ok(all.length >= 9);
  assert.ok(all.every((event) => event.type.startsWith("llm.")));
});

const payloads = async (callId: string) =>
  (
    await events()
      .find({ streamId: `call-${callId}` }, { sort: { version: 1 } })
      .toArray()
  ).map((event) => event.payload);

test("a cancel stops a running call with cancelled and no next model", async () => {
  mock.reply("mock/small", { kind: "hang" });
  const { taskId, callId, event } = callRequest(["mock/small", "mock/large"], {
    kind: "subtask",
    subtaskId: randomUUID(),
  });
  await publish(event);
  await waitFor(async () => (await typesOf(callId)).length === 1);
  await publish(cancelOf(taskId, callId));
  await ended(callId, 2);
  const [, failed] = await payloads(callId);
  const reserve = reserves.find((item) => item.taskId === taskId);
  assert.ok(failed && reserve);
  assert.equal(failed.errorClass, "cancelled");
  assert.equal("nextModel" in failed, false);
  assert.equal(failed.costMicroUsd, reserve.amountMicroUsd);
  assert.equal(failed.callCostMicroUsd, reserve.amountMicroUsd);
  assert.deepEqual(
    settles.filter((item) => item.reservationId === reserve.reservationId),
    [{ reservationId: reserve.reservationId, actualMicroUsd: reserve.amountMicroUsd }],
  );
});

test("a cancel that comes first answers the later CallRequested with no reserve", async () => {
  const { taskId, callId, event } = callRequest(["mock/small"]);
  await publish(cancelOf(taskId, callId));
  await waitFor(
    async () =>
      (await mongo.db.collection<{ _id: string }>("cancels").countDocuments({ _id: callId })) === 1,
  );
  const calls = mock.received.length;
  await publish(event);
  await ended(callId, 1);
  const [failed] = await payloads(callId);
  assert.ok(failed);
  assert.equal(failed.errorClass, "cancelled");
  assert.equal(failed.costMicroUsd, 0);
  assert.equal(failed.callCostMicroUsd, 0);
  assert.equal(reserves.filter((item) => item.taskId === taskId).length, 0);
  assert.equal(mock.received.length, calls);
});

test("a cancel stored by another pod stops the call at the next heartbeat", async () => {
  mock.reply("mock/small", { kind: "hang" });
  const { callId, event } = callRequest(["mock/small"]);
  await publish(event);
  await waitFor(async () => (await typesOf(callId)).length === 1);
  await mongo.db
    .collection<{ _id: string; createdAt: Date }>("cancels")
    .insertOne({ _id: callId, createdAt: new Date() });
  await ended(callId, 2);
  assert.equal((await payloads(callId))[1]?.errorClass, "cancelled");
});
