import assert from "node:assert/strict";
import { createServer } from "node:http";
import type { AddressInfo } from "node:net";
import { after, before, test } from "node:test";
import {
  classify,
  failure,
  maxErrorBytes,
  maxStoredTextBytes,
  openRouter,
} from "../../src/adapters/openrouter.ts";
import {
  errorReply,
  startMockOpenRouter,
  streamError,
  streamOf,
  usage,
  type MockOpenRouter,
} from "../mock-openrouter.ts";

let mock: MockOpenRouter;
const fakeKey = "test-key-not-real";

before(async () => {
  mock = await startMockOpenRouter();
});

after(async () => {
  await mock.close();
});

const provider = (zdr = true) => openRouter({ baseUrl: `${mock.url}/`, apiKey: fakeKey, zdr });

const call = (model: string, signal = AbortSignal.timeout(5000)) => {
  const deltas: string[] = [];
  const done = provider().complete({
    model,
    messages: [{ role: "user", content: "Hello" }],
    maxTokens: 64,
    maxPrice: { prompt: 1, completion: 2, request: 0 },
    signal,
    onDelta: (text) => deltas.push(text),
  });
  return { deltas, done };
};

test("a stream gives the deltas, the text, the finish reason and the usage", async () => {
  mock.reply("mock/a", streamOf("mock/a", ["Hel", "lo", " world"], { finish: "length" }));
  const { deltas, done } = call("mock/a");
  const { result, response } = await done;
  assert.deepEqual(deltas, ["Hel", "lo", " world"]);
  assert.deepEqual(result, {
    ok: true,
    text: "Hello world",
    finishReason: "length",
    usage: { inputTokens: 10, outputTokens: 3, cachedTokens: 0, costUsd: 0.000042 },
  });
  assert.equal(response.generationId, "gen-mock-1");
  assert.equal(response.status, 200);
});

test("the request has the privacy flags, the price cap, max_tokens and no app headers", async () => {
  const { done } = call("mock/b");
  const { request } = await done;
  const received = mock.received.at(-1);
  assert.ok(received);
  assert.equal(received.headers.authorization, `Bearer ${fakeKey}`);
  assert.equal(received.headers["http-referer"], undefined);
  assert.equal(received.headers["x-title"], undefined);
  assert.equal(received.headers["x-openrouter-title"], undefined);
  assert.deepEqual(received.body, {
    model: "mock/b",
    messages: [{ role: "user", content: "Hello" }],
    max_tokens: 64,
    stream: true,
    service_tier: "default",
    provider: {
      data_collection: "deny",
      zdr: true,
      max_price: { prompt: 1, completion: 2, request: 0 },
    },
  });
  assert.deepEqual(request, received.body);
  assert.equal(JSON.stringify(request).includes(fakeKey), false);
});

test("zdr can be turned off", async () => {
  await provider(false).complete({
    model: "mock/c",
    messages: [{ role: "user", content: "Hello" }],
    maxTokens: 1,
    maxPrice: { prompt: 0, completion: 0, request: 0 },
    signal: AbortSignal.timeout(5000),
    onDelta: () => undefined,
  });
  assert.deepEqual(mock.received.at(-1)?.body.provider, {
    data_collection: "deny",
    max_price: { prompt: 0, completion: 0, request: 0 },
  });
});

test("a mid-stream error with HTTP 200 is a failure that keeps the partial text", async () => {
  mock.reply(
    "mock/d",
    streamError("mock/d", ["part"], {
      code: 429,
      message: "Rate limit exceeded",
      metadata: { error_type: "rate_limit_exceeded" },
    }),
  );
  const { deltas, done } = call("mock/d");
  const { result, response } = await done;
  assert.deepEqual(deltas, ["part"]);
  assert.deepEqual(result, {
    ok: false,
    errorClass: "rate_limit",
    reason: "OpenRouter stream error 429 rate_limit_exceeded (rate_limit)",
    committed: true,
    text: "part",
  });
  assert.deepEqual(response.error, {
    code: 429,
    message: "Rate limit exceeded",
    metadata: { error_type: "rate_limit_exceeded" },
  });
});

test("an error event as the first and only event is a provider failure", async () => {
  mock.reply("mock/e", streamError("mock/e", [], { code: "server_error", message: "boom" }));
  const { result } = await call("mock/e").done;
  assert.deepEqual(result, {
    ok: false,
    errorClass: "provider",
    reason: "OpenRouter stream error server_error (provider)",
    committed: true,
    text: "",
  });
});

test("a finish reason error with no error object is a provider failure", async () => {
  mock.reply("mock/f", {
    kind: "stream",
    events: [{ choices: [{ delta: { content: "x" }, finish_reason: "error" }] }],
  });
  const { result } = await call("mock/f").done;
  assert.equal(!result.ok && result.errorClass, "provider");
});

test("HTTP errors map to the error classes and are not committed", async () => {
  const cases: [Parameters<typeof errorReply>, string][] = [
    [[429, "rate_limit_exceeded"], "rate_limit"],
    [[402, "payment_required", { limit_source: "openrouter_credits" }], "budget_refused"],
    [[402, "payment_required", { limit_source: "openrouter_in_flight_budget" }], "rate_limit"],
    [[400, "context_length_exceeded"], "invalid"],
    [[401, "authentication"], "invalid"],
    [[500, "server"], "provider"],
    [[502, "provider_unavailable"], "provider"],
    [[503, "provider_overloaded"], "provider"],
    [[504, "timeout"], "timeout"],
    [[404, "not_found"], "provider"],
    [[500, "rate_limit_exceeded", { limit_source: null }], "rate_limit"],
  ];
  for (const [args, errorClass] of cases) {
    mock.reply("mock/g", errorReply(...args));
    const { result } = await call("mock/g").done;
    assert.equal(!result.ok && result.errorClass, errorClass, String(args[0]));
    assert.equal(!result.ok && result.committed, false);
  }
  mock.reply("mock/g", { kind: "json", status: 503, body: "not json" });
  const { result } = await call("mock/g").done;
  assert.equal(!result.ok && result.reason, "OpenRouter error 503 (provider)");
});

test("a stream that ends with no finish and no [DONE] is a provider failure", async () => {
  mock.reply("mock/h", {
    kind: "stream",
    events: [{ choices: [{ delta: { content: "half" }, finish_reason: null }] }],
  });
  const { result } = await call("mock/h").done;
  assert.deepEqual(result, {
    ok: false,
    errorClass: "provider",
    reason: "OpenRouter stream ended early (provider)",
    committed: true,
    text: "half",
  });
});

test("a chunk that is not valid JSON is a provider failure", async () => {
  mock.reply("mock/i", { kind: "stream", events: ["{not json"] });
  const { result } = await call("mock/i").done;
  assert.equal(!result.ok && result.reason, "OpenRouter stream chunk (provider)");
});

test("usage with no cost keeps the tokens for the catalog price", async () => {
  mock.reply("mock/j", streamOf("mock/j", ["a"], { usage: usage(5, 1) }));
  const { result } = await call("mock/j").done;
  assert.deepEqual(result.ok && result.usage, { inputTokens: 5, outputTokens: 1, cachedTokens: 0 });
});

test("a timeout before the response headers is committed, because the request was sent", async () => {
  mock.reply("mock/k", { kind: "hang" });
  const { result } = await call("mock/k", AbortSignal.timeout(200)).done;
  assert.deepEqual(result, {
    ok: false,
    errorClass: "timeout",
    reason: "OpenRouter did not end the call in time (timeout)",
    committed: true,
    text: "",
  });
});

test("a socket reset after the request is sent is committed", async () => {
  mock.reply("mock/reset", { kind: "reset" });
  const { result } = await call("mock/reset").done;
  assert.deepEqual(result, {
    ok: false,
    errorClass: "provider",
    reason: "The connection to OpenRouter failed (provider)",
    committed: true,
    text: "",
  });
});

test("a timeout in the middle of the stream is committed", async () => {
  const controller = new AbortController();
  mock.reply("mock/l", {
    kind: "stream",
    events: [{ choices: [{ delta: { content: "token" }, finish_reason: null }] }],
    hold: true,
  });
  const { deltas, done } = call("mock/l", controller.signal);
  setTimeout(() => {
    controller.abort();
  }, 200);
  const { result } = await done;
  assert.equal(!result.ok && result.errorClass, "timeout");
  assert.equal(!result.ok && result.committed, true);
  assert.ok(deltas.length > 0);
});

test("a server that refuses the connection is a provider failure that is not committed", async () => {
  const server = createServer();
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  const { port } = server.address() as AddressInfo;
  await new Promise((resolve) => server.close(resolve));
  const closed = openRouter({
    baseUrl: `http://127.0.0.1:${String(port)}/api/v1`,
    apiKey: fakeKey,
    zdr: true,
  });
  const { result } = await closed.complete({
    model: "mock/m",
    messages: [{ role: "user", content: "Hello" }],
    maxTokens: 1,
    maxPrice: { prompt: 0, completion: 0, request: 0 },
    signal: AbortSignal.timeout(5000),
    onDelta: () => undefined,
  });
  assert.equal(!result.ok && result.errorClass, "provider");
  assert.equal(!result.ok && result.committed, false);
});

test("the catalog keeps the models with valid prices and sends the key", async () => {
  const models = await provider().models();
  assert.deepEqual(
    models.map((model) => model.id),
    ["mock/small", "mock/large", "mock/free:free"],
  );
  assert.deepEqual(models[0], {
    id: "mock/small",
    contextLength: 8192,
    pricing: { prompt: "0.000001", completion: "0.000002" },
  });
});

test("a catalog error throws", async () => {
  const wrong = openRouter({ baseUrl: `${mock.url}/missing`, apiKey: fakeKey, zdr: true });
  await assert.rejects(wrong.models(), /404/);
});

test("classify follows the error type before the status", () => {
  assert.equal(classify(500, { metadata: { error_type: "rate_limit_exceeded" } }), "rate_limit");
  assert.equal(
    classify(403, { metadata: { limit_source: "openrouter_key_limit" } }),
    "budget_refused",
  );
  assert.equal(classify(403, { metadata: { error_type: "content_policy_violation" } }), "invalid");
  assert.equal(classify(408, undefined), "timeout");
  assert.equal(classify(524, {}), "timeout");
  assert.equal(classify(529, {}), "provider");
  assert.equal(classify(undefined, undefined), "provider");
  assert.equal(
    failure("error", 400, { metadata: { error_type: "Bad Type!" } }).reason,
    "OpenRouter error 400 (invalid)",
  );
});

test("an error body is read up to 64 KiB only", async () => {
  mock.reply("mock/n", {
    kind: "json",
    status: 500,
    body: { error: { code: 500, message: "x".repeat(maxErrorBytes * 2) } },
  });
  const { result, response } = await call("mock/n").done;
  assert.equal(!result.ok && result.errorClass, "provider");
  assert.equal(response.error, undefined);
});

test("the stored text stops at 1 MiB with a flag, the deltas go on", async () => {
  const piece = "z".repeat(400 * 1024);
  mock.reply("mock/o", streamOf("mock/o", [piece, piece, piece]));
  const { deltas, done } = call("mock/o");
  const { result, response } = await done;
  assert.equal(result.ok, true);
  assert.equal(response.truncated, true);
  assert.ok(Buffer.byteLength(response.text) <= maxStoredTextBytes);
  assert.equal(deltas.join("").length, piece.length * 3);
});
