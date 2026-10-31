import {
  deltaSubject,
  type CallRequestedEventPayloadV1,
  type DeltaV1,
  type EventEnvelopeV1,
  type ReserveFundsReplyV1,
  type ReserveFundsRequestV1,
  type SettleReservationReplyV1,
  type SettleReservationRequestV1,
} from "@famashines/ai-systems-contracts";
import type { EventStore } from "@famashines/ai-systems-kit/event-store";
import type { HandlerContext } from "@famashines/ai-systems-kit/messaging";
import type { Attempt, Attempts, Bodies, Catalog, Logger } from "../adapters/mongo.ts";
import type { LlmProvider } from "../adapters/openrouter.ts";
import {
  callStreamId,
  cleanText,
  evolve,
  initialState,
  interrupted,
  nextStep,
  outcomeOf,
  retryable,
  splitDelta,
  type CallEvent,
  type CallState,
  type Outcome,
} from "../domain/call.ts";
import { maxPrice, worstCaseMicroUsd } from "../domain/cost.ts";

export interface Budget {
  reserve(input: ReserveFundsRequestV1): Promise<ReserveFundsReplyV1>;
  settle(input: SettleReservationRequestV1): Promise<SettleReservationReplyV1>;
}

export interface Deps {
  store: EventStore;
  attempts: Attempts;
  bodies: Bodies;
  catalog: Catalog;
  budget: Budget;
  provider: LlmProvider;
  publish: (subject: string, delta: DeltaV1) => void;
  newId: () => string;
  now: () => number;
  stopping: AbortSignal;
  logger: Logger;
}

export type CallRequested = EventEnvelopeV1 & {
  type: "orchestration.CallRequested";
  payload: CallRequestedEventPayloadV1;
};

export class StoppingError extends Error {
  constructor() {
    super("The service stops; the call comes again");
    this.name = "StoppingError";
  }
}

const reserveMarginMs = 60_000;
const maxTtlMs = 3_600_000;

export async function handleCallRequested(
  deps: Deps,
  event: CallRequested,
  { markProcessed }: Pick<HandlerContext, "markProcessed">,
): Promise<void> {
  const { store } = deps;
  const call = event.payload;
  const streamId = callStreamId(call.callId);
  const history = await store.readStream(streamId);
  let state: CallState = history.reduce(
    (current, stored) => evolve(current, stored as CallEvent),
    initialState,
  );
  let version = history.length;

  const append = async (events: CallEvent[]): Promise<void> => {
    const final = events.reduce(evolve, state).final;
    await store.append({
      streamId,
      expectedVersion: version,
      events: events.map((item) => ({
        ...item,
        schemaVersion: 1,
        correlationId: event.correlationId,
        causationId: event.eventId,
        actor: { kind: "system", id: "ai-proxy" },
      })),
      ...(final ? { also: markProcessed } : {}),
    });
    state = events.reduce(evolve, state);
    version += events.length;
  };

  const base = (attempt: number) => ({
    taskId: call.taskId,
    callId: call.callId,
    attempt,
    model: call.models[attempt - 1] ?? "",
  });

  const refuse = (attempt: number, reason: string, reservationId?: string): Promise<void> =>
    append([
      {
        type: "llm.CallFailed",
        payload: {
          ...base(attempt),
          errorClass: "budget_refused",
          reason,
          costMicroUsd: 0,
          ...(reservationId === undefined ? {} : { reservationId }),
        },
      },
    ]);

  interface Done {
    entry: Attempt;
    outcome: Outcome;
    stored: boolean;
  }

  async function run(attempt: number): Promise<Done | undefined> {
    const { model } = base(attempt);
    const entry = await deps.catalog.model(model);
    if (typeof entry === "string") {
      const unavailable = entry === "unavailable";
      await append([
        {
          type: "llm.CallFailed",
          payload: {
            ...base(attempt),
            errorClass: unavailable ? "provider" : "invalid",
            reason: unavailable
              ? "The model catalog is not available"
              : "The model is not in the model catalog",
            costMicroUsd: 0,
          },
        },
      ]);
      return undefined;
    }
    const worst = worstCaseMicroUsd(entry, call.messages, call.maxTokens);
    if (call.maxCostMicroUsd !== undefined && worst > call.maxCostMicroUsd) {
      await refuse(
        attempt,
        `The worst-case cost of ${String(worst)} micro-USD is above the call limit of ${String(call.maxCostMicroUsd)} micro-USD`,
      );
      return undefined;
    }
    const stored = await deps.attempts.ensure(call.callId, attempt, {
      reservationId: deps.newId(),
      bodyId: deps.newId(),
      amountMicroUsd: worst,
    });
    let reply: ReserveFundsReplyV1;
    try {
      reply = await deps.budget.reserve({
        reservationId: stored.reservationId,
        taskId: call.taskId,
        agentId: call.agentId,
        amountMicroUsd: stored.amountMicroUsd,
        ttlMs: Math.min(call.maxDurationMs + reserveMarginMs, maxTtlMs),
      });
    } catch (error) {
      deps.logger.warn({ error: (error as Error).name }, "the budget reservation failed");
      await refuse(attempt, "The budget service did not answer", stored.reservationId);
      return undefined;
    }
    if (!reply.ok) {
      await refuse(attempt, reply.reason, stored.reservationId);
      return undefined;
    }
    await append([
      {
        type: "llm.CallStarted",
        payload: {
          ...base(attempt),
          agentId: call.agentId,
          agentVersion: call.agentVersion,
          reservationId: stored.reservationId,
        },
      },
    ]);
    let i = 0;
    const started = deps.now();
    const { result, request, response } = await deps.provider.complete({
      model,
      messages: call.messages,
      maxTokens: call.maxTokens,
      maxPrice: maxPrice(entry),
      signal: AbortSignal.any([AbortSignal.timeout(call.maxDurationMs), deps.stopping]),
      onDelta: (text) => {
        for (const part of splitDelta(cleanText(text))) {
          deps.publish(deltaSubject(call.taskId, call.callId), { attempt, i, text: part });
          i += 1;
        }
      },
    });
    if (deps.stopping.aborted && !result.ok) throw new StoppingError();
    const outcome = outcomeOf(result, entry, stored.amountMicroUsd, deps.now() - started);
    await deps.bodies.save({
      _id: stored.bodyId,
      callId: call.callId,
      attempt,
      model,
      request,
      response: { ...response, text: cleanText(response.text) },
      outcome,
      createdAt: new Date(),
    });
    return { entry: stored, outcome, stored: true };
  }

  async function resume(attempt: number): Promise<Done> {
    const entry = await deps.attempts.ensure(call.callId, attempt, {
      reservationId: deps.newId(),
      bodyId: deps.newId(),
      amountMicroUsd: 0,
    });
    const outcome = await deps.bodies.outcome(entry.bodyId);
    return outcome
      ? { entry, outcome, stored: true }
      : { entry, outcome: interrupted(entry.amountMicroUsd), stored: false };
  }

  async function settle(entry: Attempt, outcome: Outcome): Promise<void> {
    try {
      const reply = await deps.budget.settle({
        reservationId: entry.reservationId,
        actualMicroUsd: outcome.costMicroUsd,
      });
      if (reply.state === "expired" || reply.state === "unknown") {
        deps.logger.warn({ state: reply.state }, "the reservation was not open at the settle");
      }
    } catch (error) {
      deps.logger.warn(
        { error: (error as Error).name },
        "the settle failed; the reservation expires and is booked in full",
      );
    }
  }

  async function finish(attempt: number, { entry, outcome, stored }: Done): Promise<void> {
    await settle(entry, outcome);
    if (!("errorClass" in outcome)) {
      await append([
        {
          type: "llm.CallCompleted",
          payload: { ...base(attempt), ...outcome, bodyId: entry.bodyId },
        },
      ]);
      return;
    }
    const nextModel = retryable(outcome.errorClass) ? call.models[attempt] : undefined;
    await append([
      {
        type: "llm.CallFailed",
        payload: {
          ...base(attempt),
          ...outcome,
          ...(stored ? { bodyId: entry.bodyId } : {}),
          reservationId: entry.reservationId,
          ...(nextModel === undefined ? {} : { nextModel }),
        },
      },
    ]);
  }

  for (;;) {
    const step = nextStep(state, call.models.length);
    if (step.kind === "done") return;
    const done = step.kind === "resume" ? await resume(step.attempt) : await run(step.attempt);
    if (done) await finish(step.attempt, done);
  }
}
