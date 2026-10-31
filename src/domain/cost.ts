import type { CallMessage } from "@famashines/ai-systems-contracts";
import { z } from "zod";

const decimal = /^\d+(\.\d+)?$/;
const Price = z.string().max(40).regex(decimal);

export const Pricing = z.object({
  prompt: Price,
  completion: Price,
  request: Price.optional(),
  internal_reasoning: Price.optional(),
  input_cache_write: Price.optional(),
  input_cache_write_1h: Price.optional(),
  overrides: z.array(z.record(z.string(), z.unknown())).max(50).optional(),
});
export type Pricing = z.infer<typeof Pricing>;

export interface CatalogModel {
  id: string;
  contextLength?: number;
  pricing: Pricing;
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
] as const;
type PriceKey = (typeof priceKeys)[number];

const attoPerUsd = 10n ** 18n;
const attoPerMicro = 10n ** 12n;

export function atto(price: string): bigint {
  const [whole = "0", fraction = ""] = price.split(".");
  return BigInt(whole + fraction.padEnd(18, "0").slice(0, 18));
}

const max = (values: bigint[]): bigint => values.reduce((a, b) => (b > a ? b : a), 0n);

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
    request: p.request,
    input: max([p.prompt, p.input_cache_write, p.input_cache_write_1h]),
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

export const outputTokenBound = (maxTokens: number): number =>
  maxTokens + Math.ceil(maxTokens / 20);

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
