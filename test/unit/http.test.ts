import assert from "node:assert/strict";
import { after, before, test } from "node:test";
import { ModelListResultV1 } from "@famashines/ai-systems-contracts";
import type { EventStore } from "@famashines/ai-systems-kit/event-store";
import { createService } from "@famashines/ai-systems-kit/http";
import { createLogger } from "@famashines/ai-systems-kit/observability";
import { createMemoryEventStore } from "@famashines/ai-systems-kit/testing";
import { MongoNetworkError } from "mongodb";
import { maxModels, routes } from "../../src/adapters/http.ts";
import type { CatalogModel } from "../../src/domain/cost.ts";
import { envSchema } from "../../src/main.ts";

const logger = createLogger({ service: "test", release: "test", level: "silent" });
const store = createMemoryEventStore();
let down = false;
const flaky: EventStore = {
  ...store,
  readAll: (options) =>
    down ? Promise.reject(new MongoNetworkError("down")) : store.readAll(options),
};
const pricing = { prompt: "0", completion: "0" };
let userModels: CatalogModel[] | "unavailable" | Error = [
  { id: "mock/zeta", name: "Zeta", pricing, efforts: [] },
  {
    id: "mock/alpha",
    name: "Alpha",
    pricing,
    efforts: ["high", "low", "none"],
    defaultEffort: "high",
  },
  { id: "mock/beta", pricing },
  { id: "mock/fee", name: "Fee", pricing: { ...pricing, new_fee: "0.01" } },
];
const catalog = {
  userModels: () =>
    userModels instanceof Error ? Promise.reject(userModels) : Promise.resolve(userModels),
};
const app = createService({
  logger,
  routes: routes({ store: flaky, catalog }),
  drainDelayMs: 0,
});
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

test("the model list has the usable user models sorted by name, and 503 with no list", async () => {
  const response = await fetch(`${base}/internal/models`);
  assert.equal(response.status, 200);
  const body = (await response.json()) as unknown;
  assert.deepEqual(ModelListResultV1.parse(body), body);
  assert.deepEqual(body, {
    items: [
      {
        id: "mock/alpha",
        name: "Alpha",
        efforts: ["high", "low", "none"],
        defaultEffort: "high",
      },
      { id: "mock/beta", name: "mock/beta", efforts: [] },
      { id: "mock/zeta", name: "Zeta", efforts: [] },
    ],
  });
  userModels = "unavailable";
  assert.equal((await fetch(`${base}/internal/models`)).status, 503);
  userModels = new MongoNetworkError("down");
  assert.equal((await fetch(`${base}/internal/models`)).status, 503);
  userModels = new Error("bug");
  assert.equal((await fetch(`${base}/internal/models`)).status, 500);
});

test("the model list has at most the contract limit of items", async () => {
  userModels = Array.from({ length: maxModels + 1 }, (_, i) => ({
    id: `mock/m${String(i).padStart(4, "0")}`,
    pricing,
  }));
  const response = await fetch(`${base}/internal/models`);
  const body = ModelListResultV1.parse(await response.json());
  assert.equal(body.items.length, maxModels);
  assert.equal(body.items.at(-1)?.id, "mock/m0999");
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
