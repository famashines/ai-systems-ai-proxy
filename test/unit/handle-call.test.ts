import assert from "node:assert/strict";
import { after, before, beforeEach, test } from "node:test";
import type {
  DeltaV1,
  ReserveFundsReplyV1,
  ReserveFundsRequestV1,
  SettleReservationRequestV1,
} from "@famashines/ai-systems-contracts";
import { UnavailableError } from "@famashines/ai-systems-kit/http";
import { createLogger } from "@famashines/ai-systems-kit/observability";
import { createMemoryEventStore, fakeIds } from "@famashines/ai-systems-kit/testing";
import type { Attempt, CallBody } from "../../src/adapters/mongo.ts";
import { openRouter, type LlmProvider } from "../../src/adapters/openrouter.ts";
import {
  handleCallRequested,
  StoppingError,
  type CallRequested,
  type Deps,
} from "../../src/application/handle-call.ts";
import type { CatalogModel } from "../../src/domain/cost.ts";
import {
  errorReply,
  startMockOpenRouter,
  streamOf,
  type MockOpenRouter,
} from "../mock-openrouter.ts";

const logger = createLogger({ service: "test", release: "test", level: "silent" });
const taskId = "10000000-0000-4000-8000-000000000001";
const callId = "10000000-0000-4000-8000-000000000002";
const agentId = "10000000-0000-4000-8000-000000000003";

const catalogModels: Record<string, CatalogModel> = {
  "mock/small": { id: "mock/small", pricing: { prompt: "0.000001", completion: "0.000002" } },
  "mock/large": {
    id: "mock/large",
    contextLength: 128000,
    pricing: { prompt: "0.0000025", completion: "0.00001" },
  },
};

let mock: MockOpenRouter;
let world: ReturnType<typeof setup>;

before(async () => {
  mock = await startMockOpenRouter();
});

after(async () => {
  await mock.close();
});

beforeEach(() => {
  mock.received.length = 0;
  world = setup();
});

function setup(options: { provider?: LlmProvider } = {}) {
  const store = createMemoryEventStore();
  const attempts = new Map<string, Attempt>();
  const bodies = new Map<string, CallBody>();
  const reserves: ReserveFundsRequestV1[] = [];
  const settles: SettleReservationRequestV1[] = [];
  const deltas: { subject: string; delta: DeltaV1 }[] = [];
  const marked: number[] = [];
  const stop = new AbortController();
  const budget = {
    reply: (input: ReserveFundsRequestV1): Promise<ReserveFundsReplyV1> =>
      Promise.resolve({
        ok: true,
        reservationId: input.reservationId,
        expiresAt: "2026-10-31T12:00:00.000Z",
      }),
    settleFails: false,
  };
  const catalog = { state: "ready" as "ready" | "unavailable" };
  const deps: Deps = {
    store,
    attempts: {
      ensure: (id, attempt, init) => {
        const key = `${id}:${String(attempt)}`;
        if (!attempts.has(key)) attempts.set(key, init);
        return Promise.resolve(attempts.get(key) as Attempt);
      },
    },
    bodies: {
      save: (body) => {
        bodies.set(body._id, body);
        return Promise.resolve();
      },
      outcome: (bodyId) => Promise.resolve(bodies.get(bodyId)?.outcome),
    },
    catalog: {
      model: (id) =>
        Promise.resolve(
          catalog.state === "unavailable" ? "unavailable" : (catalogModels[id] ?? "unknown"),
        ),
    },
    budget: {
      reserve: (input) => {
        reserves.push(input);
        return budget.reply(input);
      },
      settle: (input) => {
        settles.push(input);
        if (budget.settleFails) return Promise.reject(new UnavailableError("no answer"));
        return Promise.resolve({
          state: "settled",
          reservationId: input.reservationId,
          actualMicroUsd: input.actualMicroUsd,
        });
      },
    },
    provider: options.provider ?? openRouter({ baseUrl: mock.url, apiKey: "test-key", zdr: true }),
    publish: (subject, delta) => deltas.push({ subject, delta }),
    newId: fakeIds(7),
    now: Date.now,
    stopping: stop.signal,
    logger,
  };
  const markProcessed = () => {
    marked.push(1);
    return Promise.resolve();
  };
  const handle = (event: CallRequested) => handleCallRequested(deps, event, { markProcessed });
  const events = async (): Promise<{ type: string; payload: Record<string, unknown> }[]> =>
    (await store.readStream(`call-${callId}`)).map(({ type, payload }) => ({ type, payload }));
  return {
    deps,
    handle,
    events,
    attempts,
    bodies,
    reserves,
    settles,
    deltas,
    marked,
    budget,
    catalog,
    stop,
  };
}

const request = (overrides: Partial<CallRequested["payload"]> = {}): CallRequested => ({
  eventId: "10000000-0000-4000-8000-0000000000aa",
  type: "orchestration.CallRequested",
  schemaVersion: 1,
  streamId: `task-${taskId}`,
  version: 3,
  occurredAt: "2026-10-31T10:00:00.000Z",
  correlationId: taskId,
  causationId: "10000000-0000-4000-8000-0000000000bb",
  actor: { kind: "system", id: "orchestration" },
  payload: {
    taskId,
    callId,
    agentId,
    agentVersion: 2,
    messages: [
      { role: "system", content: "You are a writer." },
      { role: "user", content: "Write a line." },
    ],
    models: ["mock/small", "mock/large"],
    maxTokens: 100,
    maxDurationMs: 60_000,
    ...overrides,
  },
});

test("a call reserves, streams deltas, stores the body, settles and completes", async () => {
  mock.reply("mock/small", streamOf("mock/small", ["Hello ", "wor\0ld"]));
  await world.handle(request());
  const events = await world.events();
  assert.deepEqual(
    events.map((event) => event.type),
    ["llm.CallStarted", "llm.CallCompleted"],
  );
  const [reserve] = world.reserves;
  assert.ok(reserve);
  assert.equal(reserve.amountMicroUsd, 1 * (16 + 17 + 16 + 13 + 16) + 2 * 105);
  assert.equal(reserve.ttlMs, 120_000);
  assert.deepEqual(events[0]?.payload, {
    taskId,
    callId,
    attempt: 1,
    model: "mock/small",
    agentId,
    agentVersion: 2,
    reservationId: reserve.reservationId,
  });
  const completed = events[1]?.payload as Record<string, unknown>;
  assert.equal(completed.text, "Hello world");
  assert.equal(completed.costMicroUsd, 42);
  assert.equal(completed.inputTokens, 10);
  assert.deepEqual(world.settles, [{ reservationId: reserve.reservationId, actualMicroUsd: 42 }]);
  assert.deepEqual(
    world.deltas.map(({ subject, delta }) => [subject, delta]),
    [
      [`delta.${taskId}.${callId}`, { attempt: 1, i: 0, text: "Hello " }],
      [`delta.${taskId}.${callId}`, { attempt: 1, i: 1, text: "world" }],
    ],
  );
  const body = world.bodies.get(completed.bodyId as string);
  assert.ok(body);
  assert.equal(body.response.text, "Hello world");
  assert.equal(JSON.stringify(body).includes("test-key"), false);
  assert.equal(world.marked.length, 1);
});

test("a retryable failure goes to the next model with a new attempt and a new reservation", async () => {
  mock.reply("mock/small", errorReply(429, "rate_limit_exceeded"));
  mock.reply("mock/large", streamOf("mock/large", ["Second"]));
  await world.handle(request());
  const events = await world.events();
  assert.deepEqual(
    events.map((event) => [event.type, event.payload.attempt]),
    [
      ["llm.CallStarted", 1],
      ["llm.CallFailed", 1],
      ["llm.CallStarted", 2],
      ["llm.CallCompleted", 2],
    ],
  );
  const failed = events[1]?.payload as Record<string, unknown>;
  assert.equal(failed.errorClass, "rate_limit");
  assert.equal(failed.nextModel, "mock/large");
  assert.equal(failed.costMicroUsd, 0);
  assert.equal(world.reserves.length, 2);
  assert.notEqual(world.reserves[0]?.reservationId, world.reserves[1]?.reservationId);
  assert.deepEqual(
    world.settles.map((settle) => settle.actualMicroUsd),
    [0, 42],
  );
  assert.deepEqual(
    world.deltas.map(({ delta }) => delta),
    [{ attempt: 2, i: 0, text: "Second" }],
  );
  assert.equal(world.marked.length, 1);
});

test("the last model failing ends the call with no next model", async () => {
  mock.reply("mock/small", errorReply(503, "provider_overloaded"));
  mock.reply("mock/large", errorReply(500, "server"));
  await world.handle(request());
  const events = await world.events();
  const last = events.at(-1)?.payload as Record<string, unknown>;
  assert.equal(events.length, 4);
  assert.equal(last.errorClass, "provider");
  assert.equal("nextModel" in last, false);
  assert.equal(world.marked.length, 1);
});

test("a failure that is not retryable ends the call at once", async () => {
  mock.reply("mock/small", errorReply(400, "context_length_exceeded"));
  await world.handle(request());
  const events = await world.events();
  assert.equal(events.length, 2);
  const failed = events[1]?.payload as Record<string, unknown>;
  assert.equal(failed.errorClass, "invalid");
  assert.equal("nextModel" in failed, false);
  assert.equal(mock.received.length, 1);
});

test("a budget refusal gives budget_refused and no provider call", async () => {
  world.budget.reply = (input) =>
    Promise.resolve({
      ok: false,
      reservationId: input.reservationId,
      scope: { kind: "month", key: "2026-10" },
      reason: "Monthly cap reached",
    });
  await world.handle(request());
  const events = await world.events();
  assert.deepEqual(events, [
    {
      type: "llm.CallFailed",
      payload: {
        taskId,
        callId,
        attempt: 1,
        model: "mock/small",
        errorClass: "budget_refused",
        reason: "Monthly cap reached",
        costMicroUsd: 0,
        reservationId: world.reserves[0]?.reservationId,
      },
    },
  ]);
  assert.equal(mock.received.length, 0);
  assert.equal(world.settles.length, 0);
  assert.equal(world.marked.length, 1);
});

test("no budget answer fails closed with budget_refused and no provider call", async () => {
  world.budget.reply = () => Promise.reject(new UnavailableError("rpc.budget.reserve"));
  await world.handle(request());
  const events = await world.events();
  assert.equal(events.length, 1);
  assert.equal((events[0]?.payload as Record<string, unknown>).errorClass, "budget_refused");
  assert.equal(mock.received.length, 0);
});

test("a worst case above maxCostMicroUsd is refused before the reservation", async () => {
  await world.handle(request({ maxCostMicroUsd: 10 }));
  const events = await world.events();
  assert.equal(events.length, 1);
  const failed = events[0]?.payload as Record<string, unknown>;
  assert.equal(failed.errorClass, "budget_refused");
  assert.match(failed.reason as string, /above the call limit of 10 micro-USD/);
  assert.equal(world.reserves.length, 0);
  assert.equal(mock.received.length, 0);
});

test("a model that is not in the catalog is invalid; no catalog is a provider failure", async () => {
  await world.handle(request({ models: ["mock/gone"] }));
  assert.equal(
    ((await world.events())[0]?.payload as Record<string, unknown>).errorClass,
    "invalid",
  );
  world = setup();
  world.catalog.state = "unavailable";
  await world.handle(request());
  const [event] = await world.events();
  assert.equal((event?.payload as Record<string, unknown>).errorClass, "provider");
  assert.equal(world.reserves.length, 0);
  assert.equal(mock.received.length, 0);
});

test("a redelivery of a finished call does nothing", async () => {
  await world.handle(request());
  const before = (await world.events()).length;
  await world.handle(request());
  assert.equal((await world.events()).length, before);
  assert.equal(mock.received.length, 1);
  assert.equal(world.reserves.length, 1);
});

test("a redelivery before the call started reuses the reservation id", async () => {
  const store = world.deps.store;
  world.deps.store = { ...store, append: () => Promise.reject(new Error("crash")) };
  await assert.rejects(world.handle(request()), /crash/);
  world.deps.store = store;
  await world.handle(request());
  assert.equal(world.reserves.length, 2);
  assert.deepEqual(world.reserves[0], world.reserves[1]);
  assert.deepEqual(
    (await world.events()).map((event) => event.type),
    ["llm.CallStarted", "llm.CallCompleted"],
  );
});

test("a redelivery after a completed attempt uses the stored body and does not call the provider again", async () => {
  const store = world.deps.store;
  world.deps.store = {
    ...store,
    append: (input) =>
      input.events[0]?.type === "llm.CallCompleted"
        ? Promise.reject(new Error("crash before the event"))
        : store.append(input),
  };
  await assert.rejects(world.handle(request()), /crash/);
  world.deps.store = store;
  assert.equal(mock.received.length, 1);
  await world.handle(request());
  assert.equal(mock.received.length, 1);
  const events = await world.events();
  assert.deepEqual(
    events.map((event) => event.type),
    ["llm.CallStarted", "llm.CallCompleted"],
  );
  assert.equal((events[1]?.payload as Record<string, unknown>).text, "Mock answer.");
  assert.equal(world.settles.length, 2);
  assert.equal(world.settles[0]?.reservationId, world.settles[1]?.reservationId);
});

test("a redelivery after a crash in the stream books the reserved amount and goes on", async () => {
  const crashing = setup({
    provider: {
      complete: () => Promise.reject(new Error("crash in the stream")),
      models: () => Promise.resolve([]),
    },
  });
  await assert.rejects(crashing.handle(request()), /crash/);
  const deps = { ...crashing.deps, provider: world.deps.provider };
  await handleCallRequested(deps, request(), {
    markProcessed: () => Promise.resolve(),
  });
  const events = (await crashing.events()).map((event) => [event.type, event.payload.attempt]);
  assert.deepEqual(events, [
    ["llm.CallStarted", 1],
    ["llm.CallFailed", 1],
    ["llm.CallStarted", 2],
    ["llm.CallCompleted", 2],
  ]);
  const failed = (await crashing.events())[1]?.payload as Record<string, unknown>;
  assert.equal(failed.costMicroUsd, crashing.reserves[0]?.amountMicroUsd);
  assert.equal("bodyId" in failed, false);
  assert.equal(crashing.settles[0]?.actualMicroUsd, crashing.reserves[0]?.amountMicroUsd);
  assert.equal(mock.received.length, 1);
});

test("a stop during the call throws and appends no end event", async () => {
  mock.reply("mock/small", { kind: "hang" });
  const running = world.handle(request());
  setTimeout(() => {
    world.stop.abort();
  }, 50);
  await assert.rejects(running, StoppingError);
  assert.deepEqual(
    (await world.events()).map((event) => event.type),
    ["llm.CallStarted"],
  );
  assert.equal(world.bodies.size, 0);
});

test("a call that runs past maxDurationMs is a timeout", async () => {
  mock.reply("mock/small", { kind: "hang" });
  mock.reply("mock/large", { kind: "hang" });
  await world.handle(request({ maxDurationMs: 1000 }));
  const events = await world.events();
  assert.deepEqual(
    events.map((event) => event.payload.errorClass ?? event.type),
    ["llm.CallStarted", "timeout", "llm.CallStarted", "timeout"],
  );
});

test("a failed settle does not stop the call", async () => {
  world.budget.settleFails = true;
  await world.handle(request());
  assert.deepEqual(
    (await world.events()).map((event) => event.type),
    ["llm.CallStarted", "llm.CallCompleted"],
  );
});
