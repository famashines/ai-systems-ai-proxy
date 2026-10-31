import assert from "node:assert/strict";
import { after, before, test } from "node:test";
import type { EventStore } from "@famashines/ai-systems-kit/event-store";
import { createService } from "@famashines/ai-systems-kit/http";
import { createLogger } from "@famashines/ai-systems-kit/observability";
import { createMemoryEventStore } from "@famashines/ai-systems-kit/testing";
import { MongoNetworkError } from "mongodb";
import { routes } from "../../src/adapters/http.ts";
import { envSchema } from "../../src/main.ts";

const logger = createLogger({ service: "test", release: "test", level: "silent" });
const store = createMemoryEventStore();
let down = false;
const flaky: EventStore = {
  ...store,
  readAll: (options) =>
    down ? Promise.reject(new MongoNetworkError("down")) : store.readAll(options),
};
const app = createService({ logger, routes: routes({ store: flaky }), drainDelayMs: 0 });
let base = "";

before(async () => {
  const address = await app.listen(0);
  base = `http://127.0.0.1:${String(address.port)}`;
  const callId = "20000000-0000-4000-8000-000000000001";
  await store.append({
    streamId: `call-${callId}`,
    events: [
      {
        type: "llm.CallFailed",
        schemaVersion: 1,
        correlationId: callId,
        causationId: callId,
        actor: { kind: "system", id: "ai-proxy" },
        payload: {
          taskId: callId,
          callId,
          attempt: 1,
          model: "mock/small",
          errorClass: "invalid",
          reason: "The model is not in the model catalog",
          costMicroUsd: 0,
        },
      },
    ],
  });
});

after(async () => {
  await app.close();
});

test("the replay API gives the events in position order", async () => {
  const response = await fetch(`${base}/internal/events?afterPosition=0&limit=10`);
  assert.equal(response.status, 200);
  const { events } = (await response.json()) as { events: { position: number; type: string }[] };
  assert.deepEqual(
    events.map((event) => [event.position, event.type]),
    [[1, "llm.CallFailed"]],
  );
});

test("the replay API refuses a bad query and answers 503 when MongoDB is down", async () => {
  assert.equal((await fetch(`${base}/internal/events?limit=1e3`)).status, 400);
  down = true;
  assert.equal((await fetch(`${base}/internal/events`)).status, 503);
  down = false;
});

test("the config has the defaults and needs the API key", () => {
  const env = {
    MONGODB_URI: "mongodb://localhost",
    NATS_SERVERS: "nats://a, nats://b",
    NATS_USER: "ai-systems-ai-proxy",
    NATS_PASSWORD: "x",
  };
  assert.equal(envSchema.safeParse(env).success, false);
  const config = envSchema.parse({ ...env, OPENROUTER_API_KEY: "k" });
  assert.equal(config.MONGODB_DATABASE, "ai_proxy");
  assert.equal(config.OPENROUTER_BASE_URL, "https://openrouter.ai/api/v1");
  assert.equal(config.OPENROUTER_ZDR, "true");
  assert.deepEqual(config.NATS_SERVERS, ["nats://a", "nats://b"]);
  const base = (url: string) =>
    envSchema.safeParse({ ...env, OPENROUTER_API_KEY: "k", OPENROUTER_BASE_URL: url }).success;
  assert.equal(base("https://openrouter.example/api/v1"), true);
  assert.equal(base("http://127.0.0.1:8090/api/v1"), true);
  assert.equal(base("http://localhost:8090/api/v1"), true);
  assert.equal(base("http://mock:8090/api/v1"), false);
  assert.equal(base("ftp://localhost/api/v1"), false);
});
