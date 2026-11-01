import { setTimeout as sleep } from "node:timers/promises";
import {
  deltaSubject,
  type CallCancelRequestedEventPayloadV1,
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
import type { Attempt, Attempts, Bodies, Cancels, Catalog, Logger } from "../adapters/mongo.ts";
import type { LlmProvider } from "../adapters/openrouter.ts";
import {
  callStreamId,
  cancelled,
  cancelReason,
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
import { maxPrice, outputTokenBound, uncoveredPrice, worstCaseMicroUsd } from "../domain/cost.ts";

export interface Budget {
  reserve(input: ReserveFundsRequestV1): Promise<ReserveFundsReplyV1>;
  settle(input: SettleReservationRequestV1): Promise<SettleReservationReplyV1>;
}

export interface Deps {
  store: EventStore;
  attempts: Attempts;
  bodies: Bodies;
  cancels: Cancels;
  calls: Map<string, AbortController>;
  catalog: Pick<Catalog, "model">;
  budget: Budget;
  provider: Pick<LlmProvider, "complete">;
  publish: (subject: string, delta: DeltaV1) => void;
  newId: () => string;
  now: () => number;
  stopping: AbortSignal;
  logger: Logger;
  heartbeatMs?: number;
}

export type CallRequested = EventEnvelopeV1 & {
  type: "orchestration.CallRequested";
  payload: CallRequestedEventPayloadV1;
};

export type CallCancelRequested = EventEnvelopeV1 & {
  type: "orchestration.CallCancelRequested";
  payload: CallCancelRequestedEventPayloadV1;
};

export class StoppingError extends Error {
  constructor() {
    super("The service stops; the call comes again");
    this.name = "StoppingError";
  }
}

const stopped = (deps: Deps): boolean => deps.stopping.aborted;

export const reserveMarginMs = 60_000;
export const maxCallMs = 3_540_000;
export const heartbeatMs = 10_000;
export const heartbeatStaleMs = 30_000;
export const staleAfterMs = 15 * 60_000;
export const staleReason = "The request is older than 15 minutes, so the call stops";
const saveTries = 3;

export async function handleCallCancelRequested(
  deps: Pick<Deps, "cancels" | "calls">,
  event: CallCancelRequested,
): Promise<void> {
  await deps.cancels.add(event.payload.callId);
  deps.calls.get(event.payload.callId)?.abort();
}

export async function handleCallRequested(
  deps: Deps,
  event: CallRequested,
  context: Pick<HandlerContext, "markProcessed">,
): Promise<void> {
  if (stopped(deps)) throw new StoppingError();
  const { callId } = event.payload;
  const cancel = new AbortController();
  deps.calls.set(callId, cancel);
  try {
    await runCall(deps, event, context, cancel);
  } finally {
    if (deps.calls.get(callId) === cancel) deps.calls.delete(callId);
  }
}

async function runCall(
  deps: Deps,
  event: CallRequested,
  { markProcessed }: Pick<HandlerContext, "markProcessed">,
  cancel: AbortController,
): Promise<void> {
  const { store } = deps;
  const call = event.payload;
  const durationMs = Math.min(call.maxDurationMs, maxCallMs);
  const streamId = callStreamId(call.callId);
  const history = await store.readStream(streamId);
  const past = history.map((stored) => stored as CallEvent);
  let state: CallState = past.reduce(evolve, initialState);
  let version = history.length;
  const failedCost = (events: CallEvent[]): number =>
    events.reduce(
      (sum, item) => (item.type === "llm.CallFailed" ? sum + item.payload.costMicroUsd : sum),
      0,
    );
  let spent = failedCost(past);
  const startedAt = Date.parse(
    history.findLast((stored) => stored.type === "llm.CallStarted")?.occurredAt ?? "",
  );

  const append = async (events: CallEvent[]): Promise<void> => {
    const final = events.reduce(evolve, state).final;
    const last = events.at(-1);
    if (final && last) Object.assign(last.payload, await deps.attempts.totals(call.callId));
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
    spent += failedCost(events);
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

  const fail = (
    attempt: number,
    errorClass: "invalid" | "provider" | "timeout" | "cancelled",
    reason: string,
    reservationId?: string,
  ) =>
    append([
      {
        type: "llm.CallFailed",
        payload: {
          ...base(attempt),
          errorClass,
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

  async function settleQuietly(reservationId: string): Promise<void> {
    try {
      await deps.budget.settle({ reservationId, actualMicroUsd: 0 });
    } catch (error) {
      deps.logger.warn(
        { error: (error as Error).name },
        "the settle of an unanswered reservation failed; it expires",
      );
    }
  }

  async function saveBody(body: Parameters<Bodies["save"]>[0]): Promise<void> {
    for (let tries = 1; ; tries += 1) {
      try {
        await deps.bodies.save(body);
        return;
      } catch (error) {
        if (tries >= saveTries) throw error;
        await sleep(100 * tries);
      }
    }
  }

  async function run(attempt: number): Promise<Done | undefined> {
    const cancelledCall = await deps.cancels.has(call.callId);
    if (cancelledCall || deps.now() - Date.parse(event.occurredAt) > staleAfterMs) {
      const held = await deps.attempts.reservationId(call.callId, attempt);
      if (held !== undefined) await settleQuietly(held);
      await (cancelledCall
        ? fail(attempt, "cancelled", cancelReason, held)
        : fail(attempt, "timeout", staleReason, held));
      return undefined;
    }
    const { model } = base(attempt);
    const entry = await deps.catalog.model(model);
    if (typeof entry === "string") {
      await (entry === "unavailable"
        ? fail(attempt, "provider", "The model catalog is not available")
        : fail(attempt, "invalid", "The model is not in the model catalog"));
      return undefined;
    }
    const extra = uncoveredPrice(entry.pricing);
    if (extra !== undefined) {
      const key = /^[a-z0-9_]{1,64}$/.test(extra) ? extra : "an unknown key";
      await fail(
        attempt,
        "invalid",
        `The model has a price for ${key} that the cost check does not cover`,
      );
      return undefined;
    }
    const { effort } = call;
    if (effort !== undefined && !(entry.efforts ?? []).includes(effort)) {
      await fail(attempt, "invalid", `The model has no reasoning effort ${effort}`);
      return undefined;
    }
    if (effort !== undefined && effort !== "none" && call.maxTokens <= 1024) {
      await fail(attempt, "invalid", "A reasoning effort needs maxTokens above 1024");
      return undefined;
    }
    const worst = worstCaseMicroUsd(entry, call.messages, call.maxTokens);
    if (call.maxCostMicroUsd !== undefined && worst > call.maxCostMicroUsd - spent) {
      await refuse(
        attempt,
        `The worst-case cost of ${String(worst)} micro-USD is above the rest of the call limit: ${String(Math.max(0, call.maxCostMicroUsd - spent))} of ${String(call.maxCostMicroUsd)} micro-USD`,
      );
      return undefined;
    }
    const stored = await deps.attempts.ensure(call.callId, attempt, {
      reservationId: deps.newId(),
      bodyId: deps.newId(),
      amountMicroUsd: worst,
      maxPrice: maxPrice(entry),
    });
    if (stopped(deps)) throw new StoppingError();
    let reply: ReserveFundsReplyV1;
    try {
      reply = await deps.budget.reserve({
        reservationId: stored.reservationId,
        taskId: call.taskId,
        agentId: call.agentId,
        amountMicroUsd: stored.amountMicroUsd,
        ttlMs: durationMs + reserveMarginMs,
      });
    } catch (error) {
      deps.logger.warn({ error: (error as Error).name }, "the budget reservation failed");
      await settleQuietly(stored.reservationId);
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
    if (await deps.cancels.has(call.callId).catch(() => false)) cancel.abort();
    const beat = (): void => {
      deps.attempts.beat(call.callId, attempt).catch((error: unknown) => {
        deps.logger.warn({ error: (error as Error).name }, "the attempt heartbeat failed");
      });
      deps.cancels.has(call.callId).then(
        (found) => {
          if (found) cancel.abort();
        },
        (error: unknown) => {
          deps.logger.warn({ error: (error as Error).name }, "the cancel check failed");
        },
      );
    };
    beat();
    const heartbeat = setInterval(beat, deps.heartbeatMs ?? heartbeatMs);
    const { result, request, response } = await deps.provider
      .complete({
        model,
        messages: call.messages,
        maxTokens: call.maxTokens,
        ...(effort === undefined ? {} : { effort }),
        maxPrice: stored.maxPrice,
        signal: AbortSignal.any([AbortSignal.timeout(durationMs), deps.stopping, cancel.signal]),
        onDelta: (text) => {
          for (const part of splitDelta(cleanText(text))) {
            deps.publish(deltaSubject(call.taskId, call.callId), { attempt, i, text: part });
            i += 1;
          }
        },
      })
      .finally(() => {
        clearInterval(heartbeat);
      });
    const bound = outputTokenBound(call.maxTokens);
    if (result.usage && result.usage.outputTokens > bound) {
      deps.logger.warn(
        { model, outputTokens: result.usage.outputTokens, bound },
        "the output tokens are above the output bound of the worst case",
      );
    }
    const aborted = !result.ok && result.aborted === true;
    const stopping = aborted && stopped(deps);
    const ended = stopping
      ? interrupted(stored.amountMicroUsd)
      : outcomeOf(result, entry, stored.amountMicroUsd, deps.now() - started);
    const outcome = aborted && cancel.signal.aborted ? cancelled(ended) : ended;
    const body = {
      _id: stored.bodyId,
      callId: call.callId,
      attempt,
      model,
      request,
      response: { ...response, text: cleanText(response.text) },
      outcome,
      createdAt: new Date(),
    };
    if (stopping) {
      await saveBody(body).catch((error: unknown) => {
        deps.logger.warn(
          { error: (error as Error).name },
          "the body of a stopped attempt was not saved",
        );
      });
      throw new StoppingError();
    }
    await saveBody(body);
    return { entry: stored, outcome, stored: true };
  }

  async function resume(attempt: number): Promise<Done | "again"> {
    const entry = await deps.attempts.ensure(call.callId, attempt, {
      reservationId: deps.newId(),
      bodyId: deps.newId(),
      amountMicroUsd: 0,
      maxPrice: { prompt: 0, completion: 0, request: 0 },
    });
    const outcome = await deps.bodies.outcome(entry.bodyId);
    if (outcome) return { entry, outcome, stored: true };
    const wait = (entry.heartbeatAt ?? startedAt) + heartbeatStaleMs - deps.now();
    if (wait > 0) {
      try {
        await sleep(wait, undefined, { signal: deps.stopping });
      } catch {
        throw new StoppingError();
      }
      return "again";
    }
    return { entry, outcome: interrupted(entry.amountMicroUsd), stored: false };
  }

  async function settle(entry: Attempt, outcome: Outcome): Promise<void> {
    const reply = await deps.budget.settle({
      reservationId: entry.reservationId,
      actualMicroUsd: outcome.costMicroUsd,
    });
    if (reply.state === "expired" || reply.state === "unknown") {
      deps.logger.warn({ state: reply.state }, "the reservation was not open at the settle");
    }
  }

  async function finish(attempt: number, done: Done): Promise<void> {
    const { entry, stored } = done;
    await settle(entry, done.outcome);
    await deps.attempts.record(call.callId, attempt, {
      inputTokens: done.outcome.inputTokens ?? 0,
      outputTokens: done.outcome.outputTokens ?? 0,
      costMicroUsd: done.outcome.costMicroUsd,
    });
    const outcome =
      "errorClass" in done.outcome &&
      retryable(done.outcome.errorClass) &&
      (await deps.cancels.has(call.callId))
        ? cancelled(done.outcome)
        : done.outcome;
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
    if (done === "again") return runCall(deps, event, { markProcessed }, cancel);
    if (done) await finish(step.attempt, done);
  }
}
