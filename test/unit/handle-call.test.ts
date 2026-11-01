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
import { createMemoryEventStore, fakeIds, waitFor } from "@famashines/ai-systems-kit/testing";
import type { Attempt, AttemptCost, CallBody } from "../../src/adapters/mongo.ts";
import { openRouter, type LlmProvider } from "../../src/adapters/openrouter.ts";
import { cancelReason, interrupted } from "../../src/domain/call.ts";
import {
  handleCallCancelRequested,
  handleCallRequested,
  heartbeatStaleMs,
  staleAfterMs,
  staleReason,
  StoppingError,
  type CallCancelRequested,
  type CallRequested,
  type Deps,
} from "../../src/application/handle-call.ts";
import type { CatalogModel } from "../../src/domain/cost.ts";
import {
  errorReply,
  startMockOpenRouter,
  streamError,
  streamOf,
  usage,
  type MockOpenRouter,
} from "../mock-openrouter.ts";

const zeroTotals = { callInputTokens: 0, callOutputTokens: 0, callCostMicroUsd: 0 };
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
  "mock/small-b": { id: "mock/small-b", pricing: { prompt: "0.000001", completion: "0.000002" } },
  "mock/new-fee": {
    id: "mock/new-fee",
    pricing: { prompt: "0.000001", completion: "0.000002", new_fee: "0.01" },
  },
  "mock/vision": {
    id: "mock/vision",
    pricing: { prompt: "0.000001", completion: "0.000002", image: "0.001" },
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

function setup(options: { provider?: LlmProvider; cancels?: Set<string> } = {}) {
  const store = createMemoryEventStore();
  const attempts = new Map<string, Attempt & Partial<AttemptCost>>();
  const cancels = options.cancels ?? new Set<string>();
  const calls = new Map<string, AbortController>();
  const bodies = new Map<string, CallBody>();
  const reserves: ReserveFundsRequestV1[] = [];
  const settles: SettleReservationRequestV1[] = [];
  const deltas: { subject: string; delta: DeltaV1 }[] = [];
  const marked: number[] = [];
  const beats: number[] = [];
  const stop = new AbortController();
  const budget = {
    reply: (input: ReserveFundsRequestV1): Promise<ReserveFundsReplyV1> =>
      Promise.resolve({
        ok: true,
        reservationId: input.reservationId,
        expiresAt: "2026-10-31T12:00:00.000Z",
      }),
    settleFails: 0,
    saveFails: 0,
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
      beat: (id, attempt) => {
        const entry = attempts.get(`${id}:${String(attempt)}`);
        if (entry) entry.heartbeatAt = Date.now();
        beats.push(Date.now());
        return Promise.resolve();
      },
      reservationId: (id, attempt) =>
        Promise.resolve(attempts.get(`${id}:${String(attempt)}`)?.reservationId),
      record: (id, attempt, cost) => {
        Object.assign(attempts.get(`${id}:${String(attempt)}`) ?? {}, cost);
        return Promise.resolve();
      },
      totals: (id) => {
        const mine = [...attempts].filter(([key]) => key.startsWith(`${id}:`));
        const sum = (pick: (entry: Partial<AttemptCost>) => number | undefined) =>
          mine.reduce((total, [, entry]) => total + (pick(entry) ?? 0), 0);
        return Promise.resolve({
          callInputTokens: sum((entry) => entry.inputTokens),
          callOutputTokens: sum((entry) => entry.outputTokens),
          callCostMicroUsd: sum((entry) => entry.costMicroUsd),
        });
      },
    },
    cancels: {
      add: (id) => {
        cancels.add(id);
        return Promise.resolve();
      },
      has: (id) => Promise.resolve(cancels.has(id)),
    },
    calls,
    bodies: {
      save: (body) => {
        if (budget.saveFails > 0) {
          budget.saveFails -= 1;
          return Promise.reject(new Error("MongoDB write failed"));
        }
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
        if (budget.settleFails > 0) {
          budget.settleFails -= 1;
          return Promise.reject(new UnavailableError("no answer"));
        }
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
    cancels,
    calls,
    reserves,
    settles,
    deltas,
    marked,
    beats,
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
  occurredAt: new Date().toISOString(),
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

test("an empty output is still a completed call", async () => {
  mock.reply("mock/small", streamOf("mock/small", []));
  await world.handle(request());
  const events = await world.events();
  assert.deepEqual(
    events.map((event) => event.type),
    ["llm.CallStarted", "llm.CallCompleted"],
  );
  assert.equal(events[1]?.payload.text, "");
  assert.equal(world.deltas.length, 0);
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
        ...zeroTotals,
        reservationId: world.reserves[0]?.reservationId,
      },
    },
  ]);
  assert.equal(mock.received.length, 0);
  assert.equal(world.settles.length, 0);
  assert.equal(world.marked.length, 1);
});

test("no budget answer fails closed, frees the reservation with a settle of 0, and calls no provider", async () => {
  world.budget.reply = () => Promise.reject(new UnavailableError("rpc.budget.reserve"));
  await world.handle(request());
  const events = await world.events();
  assert.equal(events.length, 1);
  assert.equal(events[0]?.payload.errorClass, "budget_refused");
  assert.equal(mock.received.length, 0);
  assert.deepEqual(world.settles, [
    { reservationId: world.reserves[0]?.reservationId, actualMicroUsd: 0 },
  ]);
});

test("no budget answer and a failed settle of 0 still end the call with budget_refused", async () => {
  world.budget.reply = () => Promise.reject(new UnavailableError("rpc.budget.reserve"));
  world.budget.settleFails = 1;
  await world.handle(request());
  const events = await world.events();
  assert.equal(events[0]?.payload.errorClass, "budget_refused");
  assert.equal(world.settles.length, 1);
  assert.equal(world.marked.length, 1);
});

test("a worst case above maxCostMicroUsd is refused before the reservation", async () => {
  await world.handle(request({ maxCostMicroUsd: 10 }));
  const events = await world.events();
  assert.equal(events.length, 1);
  const failed = events[0]?.payload as Record<string, unknown>;
  assert.equal(failed.errorClass, "budget_refused");
  assert.match(failed.reason as string, /above the rest of the call limit: 10 of 10 micro-USD/);
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
  const deps = {
    ...crashing.deps,
    provider: world.deps.provider,
    now: () => Date.now() + heartbeatStaleMs + 1000,
  };
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

test("a stop during the call saves an interrupted body, throws and appends no end event", async () => {
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
  assert.deepEqual(
    [...world.bodies.values()].map((body) => body.outcome),
    [interrupted(world.reserves[0]?.amountMicroUsd ?? -1)],
  );
});

test(
  "R4: after a stop, the next delivery goes on at once, also with a long maxDurationMs",
  { timeout: 10_000 },
  async () => {
    mock.reply("mock/small", { kind: "hang" });
    const running = world.handle(request({ maxDurationMs: 600_000 }));
    setTimeout(() => {
      world.stop.abort();
    }, 50);
    await assert.rejects(running, StoppingError);
    const again = new AbortController();
    const started = Date.now();
    await handleCallRequested(
      { ...world.deps, stopping: again.signal },
      request({ maxDurationMs: 600_000 }),
      {
        markProcessed: () => Promise.resolve(),
      },
    );
    assert.ok(Date.now() - started < 2000);
    const events = await world.events();
    assert.deepEqual(
      events.map((event) => event.type),
      ["llm.CallStarted", "llm.CallFailed", "llm.CallStarted", "llm.CallCompleted"],
    );
    assert.equal(events[1]?.payload.costMicroUsd, world.reserves[0]?.amountMicroUsd);
    assert.equal(typeof events[1]?.payload.bodyId, "string");
  },
);

test(
  "R4: after a crash, the next delivery waits only until the heartbeat is 30 s old",
  { timeout: 10_000 },
  async () => {
    const crashing = setup({
      provider: {
        complete: () => Promise.reject(new Error("crash in the stream")),
        models: () => Promise.resolve([]),
      },
    });
    await assert.rejects(crashing.handle(request({ maxDurationMs: 600_000 })), /crash/);
    assert.equal(crashing.beats.length, 1);
    const deps = {
      ...crashing.deps,
      provider: world.deps.provider,
      now: () => Date.now() + heartbeatStaleMs + 1000,
    };
    const started = Date.now();
    await handleCallRequested(deps, request({ maxDurationMs: 600_000 }), {
      markProcessed: () => Promise.resolve(),
    });
    assert.ok(Date.now() - started < 2000);
    assert.deepEqual(
      (await crashing.events()).map((event) => event.type),
      ["llm.CallStarted", "llm.CallFailed", "llm.CallStarted", "llm.CallCompleted"],
    );
  },
);

test("the heartbeat is renewed while the call streams and stops after it", async () => {
  world.deps.heartbeatMs = 50;
  mock.reply("mock/small", { kind: "hang" });
  mock.reply("mock/large", streamOf("mock/large", ["ok"]));
  await world.handle(request({ maxDurationMs: 1000 }));
  const count = world.beats.length;
  assert.ok(count >= 15, String(count));
  await new Promise((resolve) => setTimeout(resolve, 200));
  assert.equal(world.beats.length, count);
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

test("a failed settle throws before the end event; the next delivery settles again from the body", async () => {
  world.budget.settleFails = 1;
  await assert.rejects(world.handle(request()), UnavailableError);
  assert.deepEqual(
    (await world.events()).map((event) => event.type),
    ["llm.CallStarted"],
  );
  await world.handle(request());
  assert.deepEqual(
    (await world.events()).map((event) => event.type),
    ["llm.CallStarted", "llm.CallCompleted"],
  );
  assert.equal(world.settles.length, 2);
  assert.deepEqual(world.settles[0], world.settles[1]);
  assert.equal(mock.received.length, 1);
});

test("a stopping service starts no call", async () => {
  let lookups = 0;
  const { catalog } = world.deps;
  world.deps.catalog = {
    model: (id) => {
      lookups += 1;
      return catalog.model(id);
    },
  };
  world.stop.abort();
  await assert.rejects(world.handle(request()), StoppingError);
  assert.equal(lookups, 0);
  assert.equal(world.reserves.length, 0);
  assert.deepEqual(await world.events(), []);
});

test("a stop before the reservation makes no reservation", async () => {
  const { catalog } = world.deps;
  world.deps.catalog = {
    model: (id) => {
      world.stop.abort();
      return catalog.model(id);
    },
  };
  await assert.rejects(world.handle(request()), StoppingError);
  assert.equal(world.reserves.length, 0);
  assert.equal(mock.received.length, 0);
});

test("maxCostMicroUsd counts the cost of the earlier attempts of the call", async () => {
  mock.reply("mock/small", streamError("mock/small", [], { code: 502, message: "down" }));
  await world.handle(request({ models: ["mock/small", "mock/small-b"], maxCostMicroUsd: 400 }));
  const events = await world.events();
  assert.deepEqual(
    events.map((event) => [event.type, event.payload.errorClass]),
    [
      ["llm.CallStarted", undefined],
      ["llm.CallFailed", "provider"],
      ["llm.CallFailed", "budget_refused"],
    ],
  );
  assert.equal(events[1]?.payload.costMicroUsd, 288);
  assert.match(events[2]?.payload.reason as string, /112 of 400 micro-USD/);
  assert.equal(world.reserves.length, 1);
  assert.equal(mock.received.length, 1);
});

test("a price that the cost check does not cover makes the model invalid", async () => {
  await world.handle(request({ models: ["mock/new-fee", "mock/small"] }));
  const events = await world.events();
  assert.equal(events.length, 1);
  assert.equal(events[0]?.payload.errorClass, "invalid");
  assert.equal(
    events[0].payload.reason,
    "The model has a price for new_fee that the cost check does not cover",
  );
  assert.equal(world.reserves.length, 0);
  world = setup();
  await world.handle(request({ models: ["mock/vision"] }));
  assert.equal((await world.events()).at(-1)?.type, "llm.CallCompleted");
});

test("a failed body save is tried again before the call goes on", async () => {
  world.budget.saveFails = 2;
  await world.handle(request());
  assert.deepEqual(
    (await world.events()).map((event) => event.type),
    ["llm.CallStarted", "llm.CallCompleted"],
  );
  assert.equal(mock.received.length, 1);
});

test("a body save that fails three times throws", async () => {
  world.budget.saveFails = 3;
  await assert.rejects(world.handle(request()), /MongoDB write failed/);
  assert.deepEqual(
    (await world.events()).map((event) => event.type),
    ["llm.CallStarted"],
  );
});

test("a started attempt with no body waits while its heartbeat is fresh", async () => {
  const crashing = setup({
    provider: {
      complete: () => Promise.reject(new Error("crash in the stream")),
      models: () => Promise.resolve([]),
    },
  });
  await assert.rejects(crashing.handle(request()), /crash/);
  const deps = {
    ...crashing.deps,
    provider: world.deps.provider,
    now: () => Date.now() + heartbeatStaleMs - 300,
  };
  const started = Date.now();
  await handleCallRequested(deps, request(), { markProcessed: () => Promise.resolve() });
  assert.ok(Date.now() - started >= 250);
  assert.deepEqual(
    (await crashing.events()).map((event) => event.type),
    ["llm.CallStarted", "llm.CallFailed", "llm.CallStarted", "llm.CallCompleted"],
  );
});

test("a stop during the heartbeat wait throws", async () => {
  const crashing = setup({
    provider: {
      complete: () => Promise.reject(new Error("crash in the stream")),
      models: () => Promise.resolve([]),
    },
  });
  await assert.rejects(crashing.handle(request()), /crash/);
  const waiting = crashing.handle(request());
  setTimeout(() => {
    crashing.stop.abort();
  }, 50);
  await assert.rejects(waiting, StoppingError);
  assert.deepEqual(
    (await crashing.events()).map((event) => event.type),
    ["llm.CallStarted"],
  );
});

test("a redelivery uses the stored max_price, not a new catalog price", async () => {
  const store = world.deps.store;
  world.deps.store = { ...store, append: () => Promise.reject(new Error("crash")) };
  await assert.rejects(world.handle(request()), /crash/);
  world.deps.store = store;
  const small = catalogModels["mock/small"];
  assert.ok(small);
  const old = small.pricing;
  small.pricing = { prompt: "0.000009", completion: "0.000009" };
  try {
    await world.handle(request());
  } finally {
    small.pricing = old;
  }
  assert.deepEqual((mock.received[0]?.body.provider as { max_price: unknown }).max_price, {
    prompt: 1,
    completion: 2,
    request: 0,
  });
});

test("a request older than 15 minutes makes no reservation and no provider call", async () => {
  const old = new Date(Date.now() - staleAfterMs - 1000).toISOString();
  await world.handle({ ...request(), occurredAt: old });
  assert.equal(world.reserves.length, 0);
  assert.equal(mock.received.length, 0);
  assert.deepEqual(await world.events(), [
    {
      type: "llm.CallFailed",
      payload: {
        taskId,
        callId,
        attempt: 1,
        model: "mock/small",
        errorClass: "timeout",
        reason: staleReason,
        costMicroUsd: 0,
        ...zeroTotals,
      },
    },
  ]);
  assert.equal(world.settles.length, 0);
  assert.equal(world.marked.length, 1);
});

test("a stale request settles a stored reservation with 0 before CallFailed", async () => {
  const reservationId = "10000000-0000-4000-8000-0000000000cc";
  world.attempts.set(`${callId}:1`, {
    reservationId,
    bodyId: "10000000-0000-4000-8000-0000000000dd",
    amountMicroUsd: 300,
    maxPrice: { prompt: 1, completion: 2, request: 0 },
  });
  const old = new Date(Date.now() - staleAfterMs - 1000).toISOString();
  await world.handle({ ...request(), occurredAt: old });
  assert.deepEqual(world.settles, [{ reservationId, actualMicroUsd: 0 }]);
  assert.equal(world.reserves.length, 0);
  assert.equal(mock.received.length, 0);
  const events = await world.events();
  assert.deepEqual(
    events.map((event) => [event.type, event.payload.errorClass, event.payload.reservationId]),
    [["llm.CallFailed", "timeout", reservationId]],
  );
});

test("a stale attempt 2 after a retryable failure of attempt 1 makes no reservation and ends the call", async () => {
  mock.reply("mock/small", errorReply(502));
  const start = Date.now();
  world.deps.now = () => start + (mock.received.length > 0 ? staleAfterMs + 1000 : 0);
  await world.handle({ ...request(), occurredAt: new Date(start).toISOString() });
  const events = await world.events();
  assert.deepEqual(
    events.map((event) => [
      event.type,
      event.payload.attempt,
      event.payload.errorClass,
      event.payload.nextModel,
    ]),
    [
      ["llm.CallStarted", 1, undefined, undefined],
      ["llm.CallFailed", 1, "provider", "mock/large"],
      ["llm.CallFailed", 2, "timeout", undefined],
    ],
  );
  assert.equal(events[2]?.payload.reason, staleReason);
  assert.equal(world.reserves.length, 1);
  assert.equal(mock.received.length, 1);
  assert.equal(world.marked.length, 1);
});

test("a provider failure at the same time as a stop keeps its class and goes to no next model", async () => {
  const real = openRouter({ baseUrl: mock.url, apiKey: "test-key", zdr: true });
  const racing = setup({
    provider: {
      complete: async (input) => {
        const call = await real.complete(input);
        racing.stop.abort();
        return call;
      },
      models: () => real.models(),
    },
  });
  mock.reply("mock/small", errorReply(400));
  await racing.handle(request());
  const events = await racing.events();
  assert.deepEqual(
    events.map((event) => [event.type, event.payload.errorClass, event.payload.nextModel]),
    [
      ["llm.CallStarted", undefined, undefined],
      ["llm.CallFailed", "invalid", undefined],
    ],
  );
  assert.equal(mock.received.length, 1);
  assert.deepEqual(
    racing.settles.map((settle) => settle.actualMicroUsd),
    [0],
  );
  assert.equal(racing.marked.length, 1);
});

const cancelRequest = (id = callId): CallCancelRequested => ({
  eventId: "10000000-0000-4000-8000-0000000000cc",
  type: "orchestration.CallCancelRequested",
  schemaVersion: 1,
  streamId: `task-${taskId}`,
  version: 9,
  occurredAt: new Date().toISOString(),
  correlationId: taskId,
  causationId: "10000000-0000-4000-8000-0000000000dd",
  actor: { kind: "system", id: "orchestration" },
  payload: { taskId, callId: id },
});

const cancelOf = (target: ReturnType<typeof setup>) =>
  handleCallCancelRequested(target.deps, cancelRequest());

test("a cancel of a running call aborts it with cancelled, the reserved amount and no next model", async () => {
  mock.reply("mock/small", { kind: "hang" });
  const running = world.handle(request());
  await waitFor(() => mock.received.length === 1);
  const started = Date.now();
  await cancelOf(world);
  await running;
  assert.ok(Date.now() - started < 1000);
  const events = await world.events();
  const reserved = world.reserves[0]?.amountMicroUsd ?? -1;
  assert.deepEqual(
    events.map((event) => event.type),
    ["llm.CallStarted", "llm.CallFailed"],
  );
  const failed = events[1]?.payload ?? {};
  assert.equal(failed.errorClass, "cancelled");
  assert.equal(failed.reason, cancelReason);
  assert.equal(failed.costMicroUsd, reserved);
  assert.equal("nextModel" in failed, false);
  assert.equal(failed.callCostMicroUsd, reserved);
  assert.deepEqual(world.settles, [
    { reservationId: world.reserves[0]?.reservationId, actualMicroUsd: reserved },
  ]);
  assert.deepEqual([...world.bodies.values()][0]?.outcome, {
    errorClass: "cancelled",
    reason: cancelReason,
    costMicroUsd: reserved,
  });
  assert.equal(mock.received.length, 1);
  assert.equal(world.calls.size, 0);
  assert.equal(world.marked.length, 1);
});

test("a cancel after the usage chunk books the real cost of the stream", async () => {
  mock.reply("mock/small", {
    kind: "stream",
    events: [
      { choices: [], usage: usage(10, 3, 0.000017) },
      { choices: [{ delta: { content: "Part" }, finish_reason: null }] },
    ],
    hold: true,
  });
  const running = world.handle(request());
  await waitFor(() => world.deltas.length === 1);
  await cancelOf(world);
  await running;
  const failed = (await world.events())[1]?.payload ?? {};
  assert.equal(failed.errorClass, "cancelled");
  assert.equal(failed.costMicroUsd, 17);
  assert.equal(failed.inputTokens, 10);
  assert.deepEqual(
    [failed.callInputTokens, failed.callOutputTokens, failed.callCostMicroUsd],
    [10, 3, 17],
  );
  assert.equal(world.settles[0]?.actualMicroUsd, 17);
});

test("a cancel that comes before its CallRequested ends the call with cost 0 and no reserve", async () => {
  await cancelOf(world);
  await world.handle(request());
  assert.deepEqual(await world.events(), [
    {
      type: "llm.CallFailed",
      payload: {
        taskId,
        callId,
        attempt: 1,
        model: "mock/small",
        errorClass: "cancelled",
        reason: cancelReason,
        costMicroUsd: 0,
        ...zeroTotals,
      },
    },
  ]);
  assert.equal(world.reserves.length, 0);
  assert.equal(world.settles.length, 0);
  assert.equal(mock.received.length, 0);
  assert.equal(world.marked.length, 1);
});

test("a cancel of a call that has ended does nothing", async () => {
  await world.handle(request());
  const before = await world.events();
  await cancelOf(world);
  await world.handle(request());
  assert.deepEqual(await world.events(), before);
  assert.equal(before.at(-1)?.type, "llm.CallCompleted");
  assert.equal(mock.received.length, 1);
});

test("a call in another pod learns the stored cancel at its next heartbeat", async () => {
  const shared = new Set<string>();
  const podA = setup({ cancels: shared });
  const podB = setup({ cancels: shared });
  podA.deps.heartbeatMs = 50;
  mock.reply("mock/small", { kind: "hang" });
  const running = podA.handle(request());
  await waitFor(() => mock.received.length === 1);
  const started = Date.now();
  await cancelOf(podB);
  assert.equal(podB.calls.size, 0);
  await running;
  assert.ok(Date.now() - started < 1000);
  const events = await podA.events();
  assert.deepEqual(
    events.map((event) => event.payload.errorClass ?? event.type),
    ["llm.CallStarted", "cancelled"],
  );
  assert.equal("nextModel" in (events[1]?.payload ?? {}), false);
  assert.equal(mock.received.length, 1);
});

test("a cancel during the reserve settles 0 and calls no provider", async () => {
  const { reply } = world.budget;
  world.budget.reply = async (input) => {
    await cancelOf(world);
    return reply(input);
  };
  await world.handle(request());
  const events = await world.events();
  assert.deepEqual(
    events.map((event) => event.payload.errorClass ?? event.type),
    ["llm.CallStarted", "cancelled"],
  );
  assert.equal(events[1]?.payload.costMicroUsd, 0);
  assert.equal(world.settles[0]?.actualMicroUsd, 0);
  assert.equal(mock.received.length, 0);
});

test("a cancel stored by another pod during the reserve settles 0 and calls no provider", async () => {
  const shared = new Set<string>();
  const podA = setup({ cancels: shared });
  const podB = setup({ cancels: shared });
  const { reply } = podA.budget;
  podA.budget.reply = async (input) => {
    await cancelOf(podB);
    return reply(input);
  };
  await podA.handle(request());
  const events = await podA.events();
  assert.deepEqual(
    events.map((event) => event.payload.errorClass ?? event.type),
    ["llm.CallStarted", "cancelled"],
  );
  const failed = events[1]?.payload ?? {};
  assert.equal(failed.costMicroUsd, 0);
  assert.equal(failed.callCostMicroUsd, 0);
  assert.equal("nextModel" in failed, false);
  assert.deepEqual(
    podA.settles.map((settle) => settle.actualMicroUsd),
    [0],
  );
  assert.equal(mock.received.length, 0);
});

test("a cancelled call that crashed in the stream ends as cancelled with no next model", async () => {
  const crashing = setup({
    provider: {
      complete: () => Promise.reject(new Error("crash in the stream")),
      models: () => Promise.resolve([]),
    },
  });
  await assert.rejects(crashing.handle(request()), /crash/);
  await cancelOf(crashing);
  const deps = { ...crashing.deps, now: () => Date.now() + heartbeatStaleMs + 1000 };
  await handleCallRequested(deps, request(), { markProcessed: () => Promise.resolve() });
  const events = await crashing.events();
  assert.deepEqual(
    events.map((event) => event.payload.errorClass ?? event.type),
    ["llm.CallStarted", "cancelled"],
  );
  assert.equal(events[1]?.payload.costMicroUsd, crashing.reserves[0]?.amountMicroUsd);
  assert.equal("nextModel" in (events[1]?.payload ?? {}), false);
});

test("the final event has the totals of all attempts; an attempt with a next model has none", async () => {
  mock.reply("mock/small", streamError("mock/small", [], { code: 502, message: "down" }));
  mock.reply("mock/large", streamOf("mock/large", ["One", "Two"]));
  await world.handle(request());
  const events = await world.events();
  const failed = events[1]?.payload ?? {};
  const completed = events[3]?.payload ?? {};
  assert.equal(failed.nextModel, "mock/large");
  assert.equal("callCostMicroUsd" in failed, false);
  assert.equal("callInputTokens" in failed, false);
  const first = failed.costMicroUsd as number;
  assert.ok(first > 0);
  assert.deepEqual(
    [completed.callInputTokens, completed.callOutputTokens, completed.callCostMicroUsd],
    [10, 2, first + 42],
  );
});

test("a final CallFailed has the totals of the earlier attempts", async () => {
  mock.reply("mock/small", streamError("mock/small", [], { code: 502, message: "down" }));
  mock.reply("mock/large", errorReply(400));
  await world.handle(request());
  const events = await world.events();
  const last = events.at(-1)?.payload ?? {};
  assert.equal(last.errorClass, "invalid");
  assert.equal(last.callCostMicroUsd, events[1]?.payload.costMicroUsd);
  assert.equal(last.callInputTokens, 0);
});

test("a CallRequested with a kind and a subtaskId runs as before", async () => {
  await world.handle(
    request({ kind: "subtask", subtaskId: "10000000-0000-4000-8000-000000000009" }),
  );
  assert.deepEqual(
    (await world.events()).map((event) => event.type),
    ["llm.CallStarted", "llm.CallCompleted"],
  );
});
