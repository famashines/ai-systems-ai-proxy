import assert from "node:assert/strict";
import { test } from "node:test";
import { createLogger } from "@famashines/ai-systems-kit/observability";
import type { Db } from "mongodb";
import { catalog } from "../../src/adapters/mongo.ts";
import type { CatalogModel } from "../../src/domain/cost.ts";

const logger = createLogger({ service: "test", release: "test", level: "silent" });
const small: CatalogModel = { id: "mock/small", pricing: { prompt: "0.000001", completion: "0" } };

function fakeDb(stored?: { fetchedAt: Date; models: CatalogModel[] }) {
  const writes: unknown[] = [];
  let doc = stored;
  const db = {
    collection: () => ({
      findOne: () => Promise.resolve(doc ?? null),
      replaceOne: (_filter: unknown, next: { fetchedAt: Date; models: CatalogModel[] }) => {
        writes.push(next);
        doc = next;
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
  });
  assert.deepEqual(await models.model("mock/small"), small);
  assert.equal(await models.model("mock/other"), "unknown");
  assert.equal(fetches, 1);
  assert.equal(writes.length, 1);
  time += 6 * 3600_000;
  await models.model("mock/small");
  assert.equal(fetches, 2);
});

test("a stored catalog is used at start, and a failed refresh keeps it", async () => {
  let time = Date.parse("2026-10-31T12:00:00Z");
  const { db } = fakeDb({ fetchedAt: new Date(time - 7 * 3600_000), models: [small] });
  let fetches = 0;
  const models = catalog({
    db,
    logger,
    now: () => time,
    fetchModels: () => {
      fetches += 1;
      return Promise.reject(new Error("down"));
    },
  });
  assert.deepEqual(await models.model("mock/small"), small);
  await models.model("mock/small");
  assert.equal(fetches, 1);
  time += 60_000;
  await models.model("mock/small");
  assert.equal(fetches, 2);
});

test("no catalog at all is unavailable", async () => {
  const { db } = fakeDb();
  const models = catalog({ db, logger, fetchModels: () => Promise.reject(new Error("down")) });
  assert.equal(await models.model("mock/small"), "unavailable");
});
