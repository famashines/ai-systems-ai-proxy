import type { ModelListResultV1 } from "@famashines/ai-systems-contracts";
import type { EventStore } from "@famashines/ai-systems-kit/event-store";
import { UnavailableError, validate, type Route } from "@famashines/ai-systems-kit/http";
import { isInfrastructure } from "@famashines/ai-systems-kit/messaging";
import { z } from "zod";
import { uncoveredPrice } from "../domain/cost.ts";
import type { Catalog } from "./mongo.ts";

const count = (fallback: string, schema: z.ZodInt) =>
  z.string().regex(/^\d+$/).default(fallback).transform(Number).pipe(schema);

const EventsQuery = z.object({
  afterPosition: count("0", z.int().min(0)),
  limit: count("100", z.int().min(1).max(1000)),
});

export const maxModels = 1000;

export function routes({
  store,
  catalog,
}: {
  store: EventStore;
  catalog: Pick<Catalog, "userModels">;
}): Route[] {
  return [
    {
      method: "GET",
      path: "/internal/events",
      handler: async (request) => {
        const { query } = await validate(request, { query: EventsQuery });
        try {
          return { body: { events: await store.readAll(query) } };
        } catch (error) {
          if (isInfrastructure(error)) throw new UnavailableError("The database is not available");
          throw error;
        }
      },
    },
    {
      method: "GET",
      path: "/internal/models",
      handler: async () => {
        const models = await catalog.userModels().catch((error: unknown) => {
          if (isInfrastructure(error)) return "unavailable" as const;
          throw error;
        });
        if (models === "unavailable") throw new UnavailableError("The model list is not available");
        const items: ModelListResultV1["items"] = models
          .filter((model) => uncoveredPrice(model.pricing) === undefined)
          .map(({ id, name, efforts, defaultEffort }) => ({
            id,
            name: name ?? id,
            efforts: efforts ?? [],
            ...(defaultEffort === undefined ? {} : { defaultEffort }),
          }))
          .sort((a, b) => a.name.localeCompare(b.name))
          .slice(0, maxModels);
        return { body: { items } };
      },
    },
  ];
}
