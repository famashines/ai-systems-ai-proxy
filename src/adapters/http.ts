import type { EventStore } from "@famashines/ai-systems-kit/event-store";
import { UnavailableError, validate, type Route } from "@famashines/ai-systems-kit/http";
import { isInfrastructure } from "@famashines/ai-systems-kit/messaging";
import { z } from "zod";

const count = (fallback: string, schema: z.ZodInt) =>
  z.string().regex(/^\d+$/).default(fallback).transform(Number).pipe(schema);

const EventsQuery = z.object({
  afterPosition: count("0", z.int().min(0)),
  limit: count("100", z.int().min(1).max(1000)),
});

export function routes({ store }: { store: EventStore }): Route[] {
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
  ];
}
