import type {
  CallCompletedEventPayloadV1,
  CallFailedEventPayloadV1,
  CallStartedEventPayloadV1,
  FinishReason,
  LlmErrorClass,
} from "@famashines/ai-systems-contracts";
import { usageMicroUsd, type CatalogModel, type Usage } from "./cost.ts";

export type CallEvent =
  | { type: "llm.CallStarted"; payload: CallStartedEventPayloadV1 }
  | { type: "llm.CallCompleted"; payload: CallCompletedEventPayloadV1 }
  | { type: "llm.CallFailed"; payload: CallFailedEventPayloadV1 };

export interface CallState {
  attempt: number;
  open: boolean;
  final: boolean;
}

export const initialState: CallState = { attempt: 0, open: false, final: false };

export const callStreamId = (callId: string): string => `call-${callId}`;

export function evolve(state: CallState, event: CallEvent): CallState {
  const { attempt } = event.payload;
  switch (event.type) {
    case "llm.CallStarted":
      return { attempt, open: true, final: false };
    case "llm.CallCompleted":
      return { attempt, open: false, final: true };
    case "llm.CallFailed":
      return { attempt, open: false, final: event.payload.nextModel === undefined };
  }
}

export type Step =
  { kind: "done" } | { kind: "start"; attempt: number } | { kind: "resume"; attempt: number };

export function nextStep(state: CallState, models: number): Step {
  if (state.final) return { kind: "done" };
  if (state.open) return { kind: "resume", attempt: state.attempt };
  if (state.attempt >= models) return { kind: "done" };
  return { kind: "start", attempt: state.attempt + 1 };
}

export const retryable = (errorClass: LlmErrorClass): boolean =>
  errorClass === "rate_limit" || errorClass === "timeout" || errorClass === "provider";

export const cleanText = (text: string): string => text.replaceAll("\0", "");

export const maxOutputChars = 100_000;

export function eventText(text: string): string {
  if (text.length <= maxOutputChars) return text;
  return text.slice(0, maxOutputChars).replace(/[\uD800-\uDBFF]$/, "");
}

const finishReasons: readonly string[] = ["stop", "length", "tool_calls", "content_filter"];

export const finishReason = (value: string | undefined): FinishReason =>
  value !== undefined && finishReasons.includes(value) ? (value as FinishReason) : "stop";

export type AttemptResult =
  | {
      ok: true;
      text: string;
      finishReason?: string;
      usage?: Usage;
    }
  | {
      ok: false;
      errorClass: LlmErrorClass;
      reason: string;
      committed: boolean;
      text: string;
      usage?: Usage;
    };

export type Outcome =
  | {
      text: string;
      finishReason: FinishReason;
      inputTokens: number;
      outputTokens: number;
      cachedTokens: number;
      costMicroUsd: number;
      latencyMs: number;
    }
  | {
      errorClass: LlmErrorClass;
      reason: string;
      costMicroUsd: number;
      inputTokens?: number;
      outputTokens?: number;
    };

export function outcomeOf(
  result: AttemptResult,
  model: CatalogModel,
  reservedMicroUsd: number,
  latencyMs: number,
): Outcome {
  const { usage } = result;
  const committed = result.ok || result.committed;
  const costMicroUsd = usage ? usageMicroUsd(model, usage) : committed ? reservedMicroUsd : 0;
  if (result.ok) {
    return {
      text: eventText(cleanText(result.text)),
      finishReason: finishReason(result.finishReason),
      inputTokens: usage?.inputTokens ?? 0,
      outputTokens: usage?.outputTokens ?? 0,
      cachedTokens: usage?.cachedTokens ?? 0,
      costMicroUsd,
      latencyMs,
    };
  }
  return {
    errorClass: result.errorClass,
    reason: result.reason,
    costMicroUsd,
    ...(usage ? { inputTokens: usage.inputTokens, outputTokens: usage.outputTokens } : {}),
  };
}

export const interrupted = (reservedMicroUsd: number): Outcome => ({
  errorClass: "provider",
  reason: "The attempt stopped before it ended; the reserved amount is booked",
  costMicroUsd: reservedMicroUsd,
});

export const deltaChars = 16_384;

export function splitDelta(text: string): string[] {
  const parts: string[] = [];
  for (let start = 0; start < text.length; start += deltaChars) {
    parts.push(text.slice(start, start + deltaChars));
  }
  return parts;
}
