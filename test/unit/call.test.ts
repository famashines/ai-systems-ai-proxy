import assert from "node:assert/strict";
import { test } from "node:test";
import {
  cleanText,
  deltaChars,
  eventText,
  evolve,
  finishReason,
  initialState,
  interrupted,
  maxOutputChars,
  nextStep,
  outcomeOf,
  retryable,
  splitDelta,
  type CallEvent,
  type CallState,
} from "../../src/domain/call.ts";
import type { CatalogModel } from "../../src/domain/cost.ts";

const ids = {
  taskId: "00000000-0000-4000-8000-000000000001",
  callId: "00000000-0000-4000-8000-000000000002",
};
const started = (attempt: number): CallEvent => ({
  type: "llm.CallStarted",
  payload: {
    ...ids,
    attempt,
    model: "mock/small",
    agentId: "00000000-0000-4000-8000-000000000003",
    agentVersion: 1,
    reservationId: "00000000-0000-4000-8000-000000000004",
  },
});
const completed = (attempt: number): CallEvent => ({
  type: "llm.CallCompleted",
  payload: {
    ...ids,
    attempt,
    model: "mock/small",
    text: "ok",
    finishReason: "stop",
    inputTokens: 1,
    outputTokens: 1,
    cachedTokens: 0,
    costMicroUsd: 1,
    latencyMs: 1,
    bodyId: "00000000-0000-4000-8000-000000000005",
  },
});
const failed = (attempt: number, nextModel?: string): CallEvent => ({
  type: "llm.CallFailed",
  payload: {
    ...ids,
    attempt,
    model: "mock/small",
    errorClass: "rate_limit",
    reason: "429",
    costMicroUsd: 0,
    ...(nextModel === undefined ? {} : { nextModel }),
  },
});

test("each state and each event give the next state", () => {
  const open: CallState = { attempt: 1, open: true, final: false };
  const cases: [CallState, CallEvent, CallState][] = [
    [initialState, started(1), open],
    [open, completed(1), { attempt: 1, open: false, final: true }],
    [open, failed(1, "mock/large"), { attempt: 1, open: false, final: false }],
    [open, failed(1), { attempt: 1, open: false, final: true }],
    [initialState, failed(1), { attempt: 1, open: false, final: true }],
    [
      { attempt: 1, open: false, final: false },
      started(2),
      { attempt: 2, open: true, final: false },
    ],
  ];
  for (const [state, event, next] of cases) assert.deepEqual(evolve(state, event), next);
});

test("the next step starts, resumes or ends the call", () => {
  assert.deepEqual(nextStep(initialState, 2), { kind: "start", attempt: 1 });
  assert.deepEqual(nextStep({ attempt: 1, open: true, final: false }, 2), {
    kind: "resume",
    attempt: 1,
  });
  assert.deepEqual(nextStep({ attempt: 1, open: false, final: false }, 2), {
    kind: "start",
    attempt: 2,
  });
  assert.deepEqual(nextStep({ attempt: 2, open: false, final: false }, 2), { kind: "done" });
  assert.deepEqual(nextStep({ attempt: 1, open: false, final: true }, 2), { kind: "done" });
});

test("only rate_limit, timeout and provider are retryable", () => {
  assert.deepEqual(
    (["rate_limit", "timeout", "provider", "invalid", "budget_refused"] as const).map(retryable),
    [true, true, true, false, false],
  );
});

test("text has no NUL and stays inside the event limit", () => {
  assert.equal(cleanText("a\0b\0"), "ab");
  assert.equal(eventText("short"), "short");
  assert.equal(eventText("x".repeat(maxOutputChars + 5)).length, maxOutputChars);
  const split = "x".repeat(maxOutputChars - 1) + "\u{1F600}";
  assert.equal(eventText(split).length, maxOutputChars - 1);
});

test("an unknown finish reason is stop", () => {
  assert.equal(finishReason("length"), "length");
  assert.equal(finishReason("function_call"), "stop");
  assert.equal(finishReason(undefined), "stop");
});

test("a long delta is split into parts of at most 16384 characters", () => {
  const parts = splitDelta("y".repeat(deltaChars * 2 + 1));
  assert.deepEqual(
    parts.map((part) => part.length),
    [deltaChars, deltaChars, 1],
  );
  assert.deepEqual(splitDelta(""), []);
});

const priced: CatalogModel = {
  id: "mock/small",
  pricing: { prompt: "0.000001", completion: "0.000002" },
};

test("a completed result gives tokens, the cost and clean text", () => {
  assert.deepEqual(
    outcomeOf(
      {
        ok: true,
        text: "hi\0",
        finishReason: "length",
        usage: { inputTokens: 1000, outputTokens: 500, cachedTokens: 7 },
      },
      priced,
      9999,
      12,
    ),
    {
      text: "hi",
      finishReason: "length",
      inputTokens: 1000,
      outputTokens: 500,
      cachedTokens: 7,
      costMicroUsd: 2000,
      latencyMs: 12,
    },
  );
});

test("a result with no usage books the reserved amount", () => {
  assert.equal(outcomeOf({ ok: true, text: "" }, priced, 777, 1).costMicroUsd, 777);
  const failedLate = outcomeOf(
    { ok: false, errorClass: "provider", reason: "r", committed: true, text: "" },
    priced,
    777,
    1,
  );
  assert.deepEqual(failedLate, { errorClass: "provider", reason: "r", costMicroUsd: 777 });
});

test("a failure before the response books nothing, a failure with usage books it", () => {
  assert.equal(
    outcomeOf(
      { ok: false, errorClass: "rate_limit", reason: "r", committed: false, text: "" },
      priced,
      777,
      1,
    ).costMicroUsd,
    0,
  );
  assert.deepEqual(
    outcomeOf(
      {
        ok: false,
        errorClass: "provider",
        reason: "r",
        committed: true,
        text: "part",
        usage: { inputTokens: 10, outputTokens: 2, cachedTokens: 0, costUsd: 0.00002 },
      },
      priced,
      777,
      1,
    ),
    { errorClass: "provider", reason: "r", costMicroUsd: 20, inputTokens: 10, outputTokens: 2 },
  );
});

test("an interrupted attempt books the reserved amount as a provider failure", () => {
  assert.deepEqual(interrupted(55), {
    errorClass: "provider",
    reason: "The attempt stopped before it ended; the reserved amount is booked",
    costMicroUsd: 55,
  });
});

test("a delta is not cut inside a surrogate pair", () => {
  const text = "a".repeat(deltaChars - 1) + "\u{1F600}" + "b";
  const parts = splitDelta(text);
  assert.deepEqual(
    parts.map((part) => part.length),
    [deltaChars - 1, 3],
  );
  assert.equal(parts.join(""), text);
});
