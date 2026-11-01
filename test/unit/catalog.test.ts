import assert from "node:assert/strict";
import { test } from "node:test";
import { createLogger } from "@famashines/ai-systems-kit/observability";
import type { Db } from "mongodb";
import { catalog } from "../../src/adapters/mongo.ts";
import type { CatalogModel } from "../../src/domain/cost.ts";

const logger = createLogger({ service: "test", release: "test", level: "silent" });
const small: CatalogModel = { id: "mock/small", pricing: { prompt: "0.000001", completion: "0" } };
const none = () => Promise.reject(new Error("down"));

interface Doc {
  fetchedAt: Date;
  models: CatalogModel[];
}

function fakeDb(stored: Record<string, Doc> = {}) {
  const writes: [string, Doc][] = [];
  const docs = { ...stored };
  const db = {
    collection: () => ({
      findOne: ({ _id }: { _id: string }) => Promise.resolve(docs[_id] ?? null),
      replaceOne: ({ _id }: { _id: string }, next: Doc) => {
        writes.push([_id, next]);
        docs[_id] = next;
        return Promise.resolve();
      },
    }),
  } as unknown as Db;
  return { db, writes };
}

test("the catalog is fetched once, stored, and used until it is 6 hours old", async () => {
  let time = 1_000_000;
  let fetches = 0;
  const { db, writes } = fakeDb();
  const models = catalog({
    db,
    logger,
    now: () => time,
    fetchModels: () => {
      fetches += 1;
      return Promise.resolve([small]);
    },
    fetchUserModels: none,
  });
  assert.deepEqual(await models.model("mock/small"), small);
  assert.equal(await models.model("mock/other"), "unknown");
  assert.equal(fetches, 1);
  assert.deepEqual(
    writes.map(([key]) => key),
    ["openrouter-v2"],
  );
  time += 6 * 3600_000;
  await models.model("mock/small");
  assert.equal(fetches, 2);
});

test("a stored catalog is used at start, and a failed refresh keeps it", async () => {
  let time = Date.parse("2026-10-31T12:00:00Z");
  const { db } = fakeDb({
    "openrouter-v2": { fetchedAt: new Date(time - 7 * 3600_000), models: [small] },
  });
  let fetches = 0;
  const models = catalog({
    db,
    logger,
    now: () => time,
    fetchModels: () => {
      fetches += 1;
      return Promise.reject(new Error("down"));
    },
    fetchUserModels: none,
  });
  assert.deepEqual(await models.model("mock/small"), small);
  await models.model("mock/small");
  assert.equal(fetches, 1);
  time += 60_000;
  await models.model("mock/small");
  assert.equal(fetches, 2);
});

test("no catalog at all is unavailable; the old catalog key is not read", async () => {
  const { db } = fakeDb({ openrouter: { fetchedAt: new Date(), models: [small] } });
  const models = catalog({ db, logger, fetchModels: none, fetchUserModels: none });
  assert.equal(await models.model("mock/small"), "unavailable");
  assert.equal(await models.userModels(), "unavailable");
});

test("the user models are a second list with their own key and cache", async () => {
  let fetches = 0;
  const { db, writes } = fakeDb();
  const models = catalog({
    db,
    logger,
    fetchModels: none,
    fetchUserModels: () => {
      fetches += 1;
      return Promise.resolve([small]);
    },
  });
  assert.deepEqual(await models.userModels(), [small]);
  assert.deepEqual(await models.userModels(), [small]);
  assert.equal(fetches, 1);
  assert.deepEqual(
    writes.map(([key]) => key),
    ["openrouter-user-v1"],
  );
  assert.equal(await models.model("mock/small"), "unavailable");
});
