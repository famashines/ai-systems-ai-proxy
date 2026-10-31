import type { Migration } from "@famashines/ai-systems-kit/mongo";
import type { Db } from "mongodb";
import type { createLogger } from "@famashines/ai-systems-kit/observability";
import type { Outcome } from "../domain/call.ts";
import type { CatalogModel } from "../domain/cost.ts";
import type { ProviderResponse } from "./openrouter.ts";

export const indexMigration: Migration = {
  name: "001-ai-proxy-indexes",
  up: async (db) => {
    await db
      .collection("call_bodies")
      .createIndex({ createdAt: 1 }, { expireAfterSeconds: 90 * 86400 }, undefined);
    await db
      .collection("attempts")
      .createIndex({ createdAt: 1 }, { expireAfterSeconds: 45 * 86400 }, undefined);
  },
};

export interface Attempt {
  reservationId: string;
  bodyId: string;
  amountMicroUsd: number;
  maxPrice: { prompt: number; completion: number; request: number };
  heartbeatAt?: number;
}

export interface Attempts {
  ensure(callId: string, attempt: number, init: Omit<Attempt, "heartbeatAt">): Promise<Attempt>;
  beat(callId: string, attempt: number): Promise<void>;
}

export interface CallBody {
  _id: string;
  callId: string;
  attempt: number;
  model: string;
  request: unknown;
  response: ProviderResponse;
  outcome: Outcome;
  createdAt: Date;
}

export interface Bodies {
  save(body: CallBody): Promise<void>;
  outcome(bodyId: string): Promise<Outcome | undefined>;
}

export interface Catalog {
  model(id: string): Promise<CatalogModel | "unknown" | "unavailable">;
}

export function mongoAttempts(db: Db): Attempts {
  const attempts = db.collection<
    Omit<Attempt, "heartbeatAt"> & { _id: string; createdAt: Date; heartbeatAt?: Date }
  >("attempts");
  const key = (callId: string, attempt: number): string => `${callId}:${String(attempt)}`;
  return {
    async ensure(callId, attempt, init) {
      const doc = await attempts.findOneAndUpdate(
        { _id: key(callId, attempt) },
        { $setOnInsert: { ...init, createdAt: new Date() } },
        { upsert: true, returnDocument: "after" },
      );
      if (!doc) throw new Error("The attempt was not stored");
      return {
        reservationId: doc.reservationId,
        bodyId: doc.bodyId,
        amountMicroUsd: doc.amountMicroUsd,
        maxPrice: doc.maxPrice,
        ...(doc.heartbeatAt ? { heartbeatAt: doc.heartbeatAt.getTime() } : {}),
      };
    },
    async beat(callId, attempt) {
      await attempts.updateOne(
        { _id: key(callId, attempt) },
        { $set: { heartbeatAt: new Date() } },
      );
    },
  };
}

export function mongoBodies(db: Db): Bodies {
  const bodies = db.collection<CallBody>("call_bodies");
  return {
    async save(body) {
      await bodies.replaceOne({ _id: body._id }, body, { upsert: true });
    },
    async outcome(bodyId) {
      return (await bodies.findOne({ _id: bodyId }))?.outcome;
    },
  };
}

export type Logger = ReturnType<typeof createLogger>;

export interface CatalogOptions {
  db: Db;
  fetchModels: () => Promise<CatalogModel[]>;
  logger: Logger;
  now?: () => number;
  maxAgeMs?: number;
  retryMs?: number;
}

export function catalog(options: CatalogOptions): Catalog {
  const { db, fetchModels, logger } = options;
  const now = options.now ?? Date.now;
  const maxAgeMs = options.maxAgeMs ?? 6 * 3600_000;
  const retryMs = options.retryMs ?? 60_000;
  const models = db.collection<{ _id: string; fetchedAt: Date; models: CatalogModel[] }>("models");
  let cache: { fetchedAt: number; byId: Map<string, CatalogModel> } | undefined;
  let lastTry = Number.NEGATIVE_INFINITY;
  const index = (list: CatalogModel[], fetchedAt: number) => ({
    fetchedAt,
    byId: new Map(list.map((model) => [model.id, model])),
  });

  async function fresh() {
    if (!cache) {
      const stored = await models.findOne({ _id: "openrouter" });
      if (stored) cache = index(stored.models, stored.fetchedAt.getTime());
    }
    const time = now();
    if ((cache && time - cache.fetchedAt < maxAgeMs) || time - lastTry < retryMs) return cache;
    lastTry = time;
    try {
      const list = await fetchModels();
      await models.replaceOne(
        { _id: "openrouter" },
        { fetchedAt: new Date(time), models: list },
        { upsert: true },
      );
      cache = index(list, time);
      logger.info({ models: list.length }, "model catalog refreshed");
    } catch (error) {
      logger.warn({ error: (error as Error).name }, "model catalog refresh failed");
    }
    return cache;
  }

  return {
    async model(id) {
      const current = await fresh();
      if (!current) return "unavailable";
      return current.byId.get(id) ?? "unknown";
    },
  };
}
