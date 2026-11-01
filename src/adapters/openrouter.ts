import { ModelId, type CallMessage, type LlmErrorClass } from "@famashines/ai-systems-contracts";
import { z } from "zod";
import type { AttemptResult } from "../domain/call.ts";
import { Pricing, type CatalogModel, type Usage } from "../domain/cost.ts";

export interface ProviderRequest {
  model: string;
  messages: readonly CallMessage[];
  maxTokens: number;
  maxPrice: { prompt: number; completion: number; request: number };
  signal: AbortSignal;
  onDelta: (text: string) => void;
}

export interface ProviderResponse {
  status?: number;
  generationId?: string;
  text: string;
  finishReason?: string;
  usage?: Usage;
  error?: unknown;
  truncated?: boolean;
}

export interface ProviderCall {
  result: AttemptResult;
  request: unknown;
  response: ProviderResponse;
}

export interface LlmProvider {
  complete(request: ProviderRequest): Promise<ProviderCall>;
  models(): Promise<CatalogModel[]>;
}

export interface OpenRouterOptions {
  baseUrl: string;
  apiKey: string;
  zdr: boolean;
  catalogTimeoutMs?: number;
}

const ErrorBody = z.object({
  code: z.union([z.number(), z.string()]).optional(),
  message: z.string().optional(),
  metadata: z
    .object({ error_type: z.string().nullish(), limit_source: z.string().nullish() })
    .nullish(),
});
type ErrorBody = z.infer<typeof ErrorBody>;

const Tokens = z.int().nonnegative();
const RawUsage = z.object({
  prompt_tokens: Tokens,
  completion_tokens: Tokens,
  prompt_tokens_details: z.object({ cached_tokens: Tokens.nullish() }).nullish(),
  cost: z.number().nonnegative().nullish(),
});

const Chunk = z.object({
  id: z.string().optional(),
  error: ErrorBody.optional(),
  usage: RawUsage.nullish(),
  choices: z
    .array(
      z.object({
        delta: z.object({ content: z.string().nullish() }).nullish(),
        finish_reason: z.string().nullish(),
        error: ErrorBody.optional(),
      }),
    )
    .default([]),
});

const ModelEntry = z.object({
  id: ModelId,
  context_length: z.int().positive().nullish(),
  pricing: Pricing,
});

const errorTypes: Readonly<Record<string, LlmErrorClass>> = {
  rate_limit_exceeded: "rate_limit",
  timeout: "timeout",
  provider_unavailable: "provider",
  provider_overloaded: "provider",
  server: "provider",
  unmapped: "provider",
};

export function classify(status: number | undefined, error: ErrorBody | undefined): LlmErrorClass {
  const type = error?.metadata?.error_type ?? undefined;
  const limitSource = error?.metadata?.limit_source ?? undefined;
  if (limitSource === "openrouter_in_flight_budget") return "rate_limit";
  if (status === 402 || limitSource !== undefined || type === "payment_required") {
    return "budget_refused";
  }
  const known = type === undefined ? undefined : errorTypes[type];
  if (known) return known;
  if (status === 429) return "rate_limit";
  if (status === 408 || status === 504 || status === 524) return "timeout";
  if (status === 404) return "provider";
  if (status !== undefined && status >= 400 && status < 500) return "invalid";
  return "provider";
}

const label = (value: string | null | undefined): string =>
  value != null && /^[a-z0-9_]{1,64}$/.test(value) ? ` ${value}` : "";

export function failure(
  where: string,
  status: number | undefined,
  error: ErrorBody | undefined,
): { errorClass: LlmErrorClass; reason: string } {
  const code = typeof error?.code === "number" ? error.code : status === 200 ? undefined : status;
  const errorClass = classify(code, error);
  const type = label(
    error?.metadata?.error_type ?? (typeof error?.code === "string" ? error.code : undefined),
  );
  return {
    errorClass,
    reason: `OpenRouter ${where}${code === undefined ? "" : ` ${String(code)}`}${type} (${errorClass})`,
  };
}

const usageOf = (raw: z.infer<typeof RawUsage>): Usage => ({
  inputTokens: raw.prompt_tokens,
  outputTokens: raw.completion_tokens,
  cachedTokens: raw.prompt_tokens_details?.cached_tokens ?? 0,
  ...(raw.cost == null ? {} : { costUsd: raw.cost }),
});

const parseJson = (text: string): unknown => {
  try {
    return JSON.parse(text) as unknown;
  } catch {
    return undefined;
  }
};

export const maxErrorBytes = 64 * 1024;
export const maxStoredTextBytes = 1024 * 1024;

const connectCodes = new Set([
  "ECONNREFUSED",
  "ENOTFOUND",
  "EAI_AGAIN",
  "EHOSTUNREACH",
  "ENETUNREACH",
  "UND_ERR_CONNECT_TIMEOUT",
  "DEPTH_ZERO_SELF_SIGNED_CERT",
  "SELF_SIGNED_CERT_IN_CHAIN",
  "UNABLE_TO_VERIFY_LEAF_SIGNATURE",
  "UNABLE_TO_GET_ISSUER_CERT_LOCALLY",
]);

export function beforeRequest(error: unknown): boolean {
  const code = (error as { cause?: { code?: unknown } } | undefined)?.cause?.code;
  return (
    typeof code === "string" &&
    (connectCodes.has(code) || code.startsWith("ERR_TLS_") || code.startsWith("CERT_"))
  );
}

async function capped(body: ReadableStream<Uint8Array> | null, max: number): Promise<string> {
  if (!body) return "";
  const parts: Uint8Array[] = [];
  let size = 0;
  try {
    for await (const piece of body) {
      parts.push(piece);
      size += piece.length;
      if (size >= max) break;
    }
  } catch {
    return "";
  }
  return new TextDecoder().decode(Buffer.concat(parts).subarray(0, max));
}

async function* lines(body: ReadableStream<Uint8Array>): AsyncGenerator<string> {
  const decoder = new TextDecoder();
  let buffer = "";
  for await (const piece of body) {
    buffer += decoder.decode(piece, { stream: true });
    const parts = buffer.split(/\r?\n/);
    buffer = parts.pop() ?? "";
    yield* parts;
  }
  if (buffer !== "") yield buffer;
}

export function openRouter(options: OpenRouterOptions): LlmProvider {
  const base = options.baseUrl.replace(/\/+$/, "");
  const auth = { authorization: `Bearer ${options.apiKey}` };

  async function complete(input: ProviderRequest): Promise<ProviderCall> {
    const request = {
      model: input.model,
      messages: input.messages,
      max_tokens: input.maxTokens,
      stream: true,
      service_tier: "default",
      provider: {
        data_collection: "deny",
        ...(options.zdr ? { zdr: true } : {}),
        max_price: input.maxPrice,
      },
    };
    const response: ProviderResponse = { text: "" };
    const done = (result: AttemptResult): ProviderCall => ({ result, request, response });
    const fail = (
      where: string,
      error: ErrorBody | undefined,
      committed: boolean,
    ): ProviderCall => {
      if (error !== undefined) response.error = error;
      return done({
        ok: false,
        ...failure(where, response.status, error),
        committed,
        text: response.text,
        ...(response.usage ? { usage: response.usage } : {}),
      });
    };
    let http: Response;
    try {
      http = await fetch(`${base}/chat/completions`, {
        method: "POST",
        headers: { ...auth, "content-type": "application/json" },
        body: JSON.stringify(request),
        signal: input.signal,
      });
    } catch (error) {
      return done(aborted(input.signal, !beforeRequest(error), response));
    }
    response.status = http.status;
    const generationId = http.headers.get("x-generation-id");
    if (generationId) response.generationId = generationId;
    if (!http.ok || !http.body) {
      const text = await capped(http.body, maxErrorBytes);
      const parsed = z.object({ error: ErrorBody }).safeParse(parseJson(text));
      return fail("error", parsed.success ? parsed.data.error : undefined, false);
    }
    let ended = false;
    let error: ErrorBody | undefined;
    let textBytes = 0;
    try {
      for await (const line of lines(http.body)) {
        if (!line.startsWith("data:")) continue;
        const data = line.slice(5).trim();
        if (data === "[DONE]") {
          ended = true;
          break;
        }
        const chunk = Chunk.safeParse(parseJson(data));
        if (!chunk.success) return fail("stream chunk", undefined, true);
        const { usage, choices } = chunk.data;
        if (usage) response.usage = usageOf(usage);
        const [choice] = choices;
        error ??= chunk.data.error ?? choice?.error;
        const content = choice?.delta?.content;
        if (content) {
          textBytes += Buffer.byteLength(content);
          if (textBytes <= maxStoredTextBytes) response.text += content;
          else response.truncated = true;
          input.onDelta(content);
        }
        if (choice?.finish_reason) {
          if (choice.finish_reason === "error") error ??= {};
          else response.finishReason = choice.finish_reason;
        }
      }
    } catch {
      return done(aborted(input.signal, true, response));
    }
    if (error !== undefined) return fail("stream error", error, true);
    if (!ended && response.finishReason === undefined) {
      return fail("stream ended early", undefined, true);
    }
    return done({
      ok: true,
      text: response.text,
      ...(response.finishReason === undefined ? {} : { finishReason: response.finishReason }),
      ...(response.usage ? { usage: response.usage } : {}),
    });
  }

  function aborted(
    signal: AbortSignal,
    committed: boolean,
    response: ProviderResponse,
  ): AttemptResult {
    const timedOut = signal.aborted;
    return {
      ok: false,
      errorClass: timedOut ? "timeout" : "provider",
      reason: timedOut
        ? "OpenRouter did not end the call in time (timeout)"
        : committed
          ? "The connection to OpenRouter failed (provider)"
          : "OpenRouter could not be reached (provider)",
      committed,
      ...(timedOut ? { aborted: true } : {}),
      text: response.text,
      ...(response.usage ? { usage: response.usage } : {}),
    };
  }

  async function models(): Promise<CatalogModel[]> {
    const http = await fetch(`${base}/models`, {
      headers: auth,
      signal: AbortSignal.timeout(options.catalogTimeoutMs ?? 10_000),
    });
    if (!http.ok) throw new Error(`The catalog request failed with ${String(http.status)}`);
    const { data } = z.object({ data: z.array(z.unknown()) }).parse(await http.json());
    return data.flatMap((entry) => {
      const parsed = ModelEntry.safeParse(entry);
      if (!parsed.success) return [];
      const { id, context_length, pricing } = parsed.data;
      return [
        { id, pricing, ...(context_length == null ? {} : { contextLength: context_length }) },
      ];
    });
  }

  return { complete, models };
}
