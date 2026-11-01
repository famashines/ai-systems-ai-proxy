import { randomUUID } from "node:crypto";
import {
  ModelId,
  ReserveFundsReplyV1,
  ReserveFundsRequestV1,
  RpcSubject,
  SettleReservationReplyV1,
  SettleReservationRequestV1,
} from "@famashines/ai-systems-contracts";
import { commonEnv, loadConfig } from "@famashines/ai-systems-kit/config";
import { createEventStore, eventStoreMigration } from "@famashines/ai-systems-kit/event-store";
import { createService } from "@famashines/ai-systems-kit/http";
import {
  connectNats,
  processedMigration,
  startConsumer,
  startRelay,
} from "@famashines/ai-systems-kit/messaging";
import { connectMongo, runMigrations } from "@famashines/ai-systems-kit/mongo";
import { createLogger, startTelemetry } from "@famashines/ai-systems-kit/observability";
import { z } from "zod";
import { routes } from "./adapters/http.ts";
import {
  cancelsMigration,
  catalog,
  indexMigration,
  mongoAttempts,
  mongoBodies,
  mongoCancels,
} from "./adapters/mongo.ts";
import { openRouter } from "./adapters/openrouter.ts";
import { handleCallCancelRequested, handleCallRequested } from "./application/handle-call.ts";

export const service = "ai-proxy";
const cancelDurable = "ai-proxy-cancel";

export const envSchema = commonEnv.omit({ SERVICE_NAME: true }).extend({
  MONGODB_URI: z.string().min(1),
  MONGODB_DATABASE: z.string().min(1).default("ai_proxy"),
  NATS_SERVERS: z
    .string()
    .min(1)
    .transform((value) =>
      value
        .split(",")
        .map((server) => server.trim())
        .filter((server) => server !== ""),
    )
    .pipe(z.array(z.string()).min(1)),
  NATS_USER: z.string().min(1),
  NATS_PASSWORD: z.string().min(1),
  OPENROUTER_API_KEY: z.string().min(1),
  OPENROUTER_BASE_URL: z
    .url()
    .refine((value) => {
      const url = new URL(value);
      return (
        url.protocol === "https:" ||
        (url.protocol === "http:" && ["localhost", "127.0.0.1"].includes(url.hostname))
      );
    }, "Use https, or http only for localhost and 127.0.0.1")
    .default("https://openrouter.ai/api/v1"),
  OPENROUTER_ZDR: z.enum(["true", "false"]).default("true"),
  MODEL_LIST: z
    .string()
    .default("")
    .transform((value) =>
      value
        .split(",")
        .map((id) => id.trim())
        .filter((id) => id !== ""),
    )
    .pipe(
      z.array(
        z.string().refine((id) => ModelId.safeParse(id).success, {
          error: (issue) => `Not a model id: ${String(issue.input)}`,
        }),
      ),
    ),
  OTEL_EXPORTER_OTLP_ENDPOINT: z.url().optional(),
  OTEL_EXPORTER_OTLP_TRACES_ENDPOINT: z.url().optional(),
});

export type Config = z.output<typeof envSchema>;

export async function start(config: Readonly<Config>, options: { drainDelayMs?: number } = {}) {
  const release = config.RELEASE;
  const telemetry = startTelemetry({ serviceName: service, release });
  const logger = createLogger({ service, release, level: config.LOG_LEVEL });
  logger.info("connecting to MongoDB");
  const mongo = await connectMongo({
    url: config.MONGODB_URI,
    database: config.MONGODB_DATABASE,
    clientOptions: { serverSelectionTimeoutMS: 5000 },
  });
  const migrations = [eventStoreMigration, processedMigration, indexMigration, cancelsMigration];
  await runMigrations(mongo.db, migrations);
  const store = createEventStore(mongo.db);
  logger.info("connecting to NATS");
  const nats = await connectNats({
    servers: config.NATS_SERVERS,
    user: config.NATS_USER,
    pass: config.NATS_PASSWORD,
    service,
    logger,
  });
  const relay = startRelay({ store, nats, streams: ["LLM"] });
  const provider = openRouter({
    baseUrl: config.OPENROUTER_BASE_URL,
    apiKey: config.OPENROUTER_API_KEY,
    zdr: config.OPENROUTER_ZDR === "true",
  });
  const stop = new AbortController();
  const models = catalog({
    db: mongo.db,
    fetchModels: () => provider.models(),
    fetchUserModels: () => provider.userModels(),
    logger,
  });
  const deps = {
    store,
    attempts: mongoAttempts(mongo.db),
    bodies: mongoBodies(mongo.db),
    cancels: mongoCancels(mongo.db),
    calls: new Map<string, AbortController>(),
    catalog: models,
    budget: {
      reserve: (input: ReserveFundsRequestV1) =>
        nats.request(RpcSubject.budgetReserve, ReserveFundsRequestV1, ReserveFundsReplyV1, input),
      settle: (input: SettleReservationRequestV1) =>
        nats.request(
          RpcSubject.budgetSettle,
          SettleReservationRequestV1,
          SettleReservationReplyV1,
          input,
        ),
    },
    provider,
    publish: (subject: string, delta: unknown) => {
      nats.connection.publish(subject, JSON.stringify(delta));
    },
    newId: randomUUID,
    now: Date.now,
    stopping: stop.signal,
    logger,
  };
  const consumer = startConsumer({
    nats,
    db: mongo.db,
    stream: "ORCHESTRATION",
    filter: "orchestration.CallRequested",
    handler: async (event, context) => {
      if (event.type === "orchestration.CallRequested") {
        await handleCallRequested(deps, event, context);
      }
    },
  });
  const cancelConsumer = startConsumer({
    nats: { ...nats, service: cancelDurable },
    db: mongo.db,
    stream: "ORCHESTRATION",
    filter: "orchestration.CallCancelRequested",
    handler: async (event) => {
      if (event.type === "orchestration.CallCancelRequested") {
        await handleCallCancelRequested(deps, event);
      }
    },
  });
  const app = createService({
    logger,
    routes: routes({ store, catalog: models, modelList: config.MODEL_LIST }),
    readiness: {
      mongo: mongo.ready,
      migrations: async () =>
        (await mongo.db
          .collection<{ _id: string }>("migrations")
          .countDocuments({ _id: { $in: migrations.map((migration) => migration.name) } })) ===
        migrations.length,
      consumer: consumer.ready,
      cancelConsumer: cancelConsumer.ready,
    },
    metrics: telemetry.metricsHandler,
    closeHooks: [
      telemetry.shutdown,
      mongo.close,
      nats.close,
      relay.close,
      async () => {
        const closing = Promise.all([consumer.close(), cancelConsumer.close()]);
        stop.abort();
        await closing;
      },
    ],
    ...options,
  });
  const address = await app.listen(config.PORT);
  logger.info({ port: address.port }, "ai-proxy started");
  return { app, address };
}

if (import.meta.main) {
  const exitEarly = (): never => process.exit(143);
  process.once("SIGTERM", exitEarly);
  process.once("SIGINT", exitEarly);
  const { app } = await start(loadConfig(envSchema));
  process.off("SIGTERM", exitEarly);
  process.off("SIGINT", exitEarly);
  app.handleSignals();
}
