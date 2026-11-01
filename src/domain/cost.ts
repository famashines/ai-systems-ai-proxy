import type { CallMessage, ReasoningEffort } from "@famashines/ai-systems-contracts";
import { z } from "zod";

const decimal = /^\d+(\.\d+)?$/;
const Price = z.string().max(40).regex(decimal);

export const Pricing = z.looseObject({
  prompt: Price,
  completion: Price,
  request: Price.optional(),
  internal_reasoning: Price.optional(),
  input_cache_write: Price.optional(),
  input_cache_write_1h: Price.optional(),
  input_cache_read: Price.optional(),
  web_search: Price.optional(),
  overrides: z.array(z.record(z.string(), z.unknown())).max(50).optional(),
});
export type Pricing = z.infer<typeof Pricing>;

export interface CatalogModel {
  id: string;
  contextLength?: number;
  pricing: Pricing;
  name?: string;
  efforts?: ReasoningEffort[];
  defaultEffort?: ReasoningEffort;
}

export interface Usage {
  inputTokens: number;
  outputTokens: number;
  cachedTokens: number;
  costUsd?: number;
}

const priceKeys = [
  "prompt",
  "completion",
  "request",
  "internal_reasoning",
  "input_cache_write",
  "input_cache_write_1h",
  "input_cache_read",
  "web_search",
] as const;
type PriceKey = (typeof priceKeys)[number];

const notCharged = new Set([
  "image",
  "audio",
  "audio_output",
  "input_audio_cache",
  "overrides",
  "min_prompt_tokens",
  "utc_start",
  "utc_end",
  "utc_days",
]);

const attoPerUsd = 10n ** 18n;
const attoPerMicro = 10n ** 12n;

export function atto(price: string): bigint {
  const [whole = "0", fraction = ""] = price.split(".");
  return BigInt(whole + fraction.padEnd(18, "0").slice(0, 18));
}

const max = (values: bigint[]): bigint => values.reduce((a, b) => (b > a ? b : a), 0n);

const charged = (value: unknown): boolean =>
  !(
    value == null ||
    value === 0 ||
    (typeof value === "string" && decimal.test(value) && atto(value) === 0n)
  );

export function uncoveredPrice(pricing: Pricing): string | undefined {
  const known: readonly string[] = priceKeys;
  return [pricing, ...(pricing.overrides ?? [])]
    .flatMap((entry) => Object.entries(entry))
    .find(([key, value]) =>
      key === "discount"
        ? Number(value) < 0
        : !known.includes(key) && !notCharged.has(key) && charged(value),
    )?.[0];
}

export function prices(pricing: Pricing): Record<PriceKey, bigint> {
  const pick = (key: PriceKey): bigint =>
    max(
      [pricing[key], ...(pricing.overrides ?? []).map((entry) => entry[key])]
        .filter((value): value is string => typeof value === "string" && decimal.test(value))
        .map(atto),
    );
  return Object.fromEntries(priceKeys.map((key) => [key, pick(key)])) as Record<PriceKey, bigint>;
}

const rates = (pricing: Pricing) => {
  const p = prices(pricing);
  return {
    request: p.request + p.web_search,
    input: max([p.prompt, p.input_cache_write, p.input_cache_write_1h, p.input_cache_read]),
    output: max([p.completion, p.internal_reasoning]),
  };
};

const toMicro = (amount: bigint): number => Number((amount + attoPerMicro - 1n) / attoPerMicro);

export const messageOverheadTokens = 16;

export function inputTokenBound(model: CatalogModel, messages: readonly CallMessage[]): number {
  const bytes = messages.reduce(
    (sum, message) => sum + Buffer.byteLength(message.content) + messageOverheadTokens,
    messageOverheadTokens,
  );
  return Math.min(bytes, model.contextLength ?? bytes);
}

export const outputTokenBound = (maxTokens: number): number => {
  const tokens = Math.max(maxTokens, 16);
  return tokens + Math.ceil(tokens / 20);
};

export function worstCaseMicroUsd(
  model: CatalogModel,
  messages: readonly CallMessage[],
  maxTokens: number,
): number {
  const { request, input, output } = rates(model.pricing);
  return toMicro(
    request +
      BigInt(inputTokenBound(model, messages)) * input +
      BigInt(outputTokenBound(maxTokens)) * output,
  );
}

export function usageMicroUsd(model: CatalogModel, usage: Usage): number {
  if (usage.costUsd !== undefined) return Math.round(usage.costUsd * 1_000_000);
  const { request, input, output } = rates(model.pricing);
  return toMicro(request + BigInt(usage.inputTokens) * input + BigInt(usage.outputTokens) * output);
}

const perMillion = (amount: bigint): number => Number(amount) / Number(attoPerMicro);

export function maxPrice(model: CatalogModel): {
  prompt: number;
  completion: number;
  request: number;
} {
  const p = prices(model.pricing);
  return {
    prompt: perMillion(p.prompt),
    completion: perMillion(p.completion),
    request: Number(p.request) / Number(attoPerUsd),
  };
}
