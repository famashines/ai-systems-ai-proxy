import assert from "node:assert/strict";
import { test } from "node:test";
import {
  atto,
  inputTokenBound,
  maxPrice,
  outputTokenBound,
  prices,
  uncoveredPrice,
  usageMicroUsd,
  worstCaseMicroUsd,
  type CatalogModel,
} from "../../src/domain/cost.ts";

const model = (pricing: CatalogModel["pricing"], contextLength?: number): CatalogModel => ({
  id: "mock/model",
  pricing,
  ...(contextLength === undefined ? {} : { contextLength }),
});

test("a price string becomes atto-USD with no float error", () => {
  assert.equal(atto("0.0000025"), 2_500_000_000_000n);
  assert.equal(atto("1"), 10n ** 18n);
  assert.equal(atto("0"), 0n);
  assert.equal(atto("0.0000000000000000019"), 1n);
});

test("prices take the highest value of the base price and all overrides", () => {
  const table = prices({
    prompt: "0.0000025",
    completion: "0.00001",
    overrides: [
      { min_prompt_tokens: 200000, prompt: "0.000005" },
      { utc_start: "0000", completion: "0.000001", request: "bad" },
    ],
  });
  assert.equal(table.prompt, atto("0.000005"));
  assert.equal(table.completion, atto("0.00001"));
  assert.equal(table.request, 0n);
  assert.equal(table.input_cache_write, 0n);
});

test("the worst case uses the bytes of the input, the output margin and the highest rates", () => {
  const messages = [{ role: "user" as const, content: "ab" }];
  assert.equal(inputTokenBound(model({ prompt: "0", completion: "0" }), messages), 2 + 16 + 16);
  assert.equal(outputTokenBound(100), 105);
  assert.equal(outputTokenBound(1), 17);
  const priced = model({
    prompt: "0.000001",
    completion: "0.000002",
    request: "0.001",
    internal_reasoning: "0.000003",
    input_cache_write: "0.00000125",
  });
  assert.equal(worstCaseMicroUsd(priced, messages, 100), 1000 + Math.ceil(34 * 1.25) + 105 * 3);
});

test("the context length caps the input tokens", () => {
  const big = [{ role: "user" as const, content: "x".repeat(10_000) }];
  assert.equal(inputTokenBound(model({ prompt: "0", completion: "0" }, 4096), big), 4096);
});

test("a free model has a worst case of zero", () => {
  const messages = [{ role: "system" as const, content: "Hello" }];
  assert.equal(worstCaseMicroUsd(model({ prompt: "0", completion: "0" }), messages, 16384), 0);
});

test("the worst case rounds up to the next micro-USD", () => {
  const messages = [{ role: "user" as const, content: "a" }];
  assert.equal(
    worstCaseMicroUsd(model({ prompt: "0.000000001", completion: "0" }), messages, 1),
    1,
  );
});

test("the reported cost wins over the token prices", () => {
  const priced = model({ prompt: "0.000001", completion: "0.000002" });
  assert.equal(
    usageMicroUsd(priced, { inputTokens: 10, outputTokens: 4, cachedTokens: 0, costUsd: 0.00014 }),
    140,
  );
  assert.equal(
    usageMicroUsd(priced, { inputTokens: 1000, outputTokens: 500, cachedTokens: 0 }),
    2000,
  );
});

test("max_price is in USD per million tokens and USD per request", () => {
  assert.deepEqual(
    maxPrice(model({ prompt: "0.0000025", completion: "0.00001", request: "0.002" })),
    { prompt: 2.5, completion: 10, request: 0.002 },
  );
});

const random = (seed: number) => () => {
  seed = (seed * 1103515245 + 12345) % 2 ** 31;
  return seed / 2 ** 31;
};

test("property: the worst case is never below the cost of an attempt within the limits", () => {
  const next = random(42);
  const price = () => (Math.floor(next() * 100_000) / 1e11).toFixed(11);
  for (let run = 0; run < 500; run += 1) {
    const pricing = { prompt: price(), completion: price(), request: price() };
    const contextLength = 1 + Math.floor(next() * 20_000);
    const priced = model(pricing, contextLength);
    const messages = Array.from({ length: 1 + Math.floor(next() * 5) }, () => ({
      role: "user" as const,
      content: "x".repeat(1 + Math.floor(next() * 5000)),
    }));
    const maxTokens = 1 + Math.floor(next() * 16384);
    const worst = worstCaseMicroUsd(priced, messages, maxTokens);
    const used = usageMicroUsd(priced, {
      inputTokens: Math.floor(next() * inputTokenBound(priced, messages)),
      outputTokens: Math.floor(next() * outputTokenBound(maxTokens)),
      cachedTokens: 0,
    });
    assert.ok(Number.isInteger(worst) && worst >= 0);
    assert.ok(used <= worst, `run ${String(run)}: ${String(used)} > ${String(worst)}`);
  }
});

test("a price key that the worst case does not cover is found, also in overrides", () => {
  assert.equal(uncoveredPrice({ prompt: "0.1", completion: "0.1" }), undefined);
  assert.equal(
    uncoveredPrice({
      prompt: "0.1",
      completion: "0.1",
      image: "0.01",
      discount: 0.1,
      new_fee: "0",
      overrides: [{ min_prompt_tokens: 200000, utc_days: [1], prompt: "0.2" }],
    }),
    undefined,
  );
  assert.equal(uncoveredPrice({ prompt: "0", completion: "0", new_fee: "0.001" }), "new_fee");
  assert.equal(uncoveredPrice({ prompt: "0", completion: "0", web_search: "0.01" }), undefined);
  for (const key of ["image_output", "image_token"]) {
    assert.equal(uncoveredPrice({ prompt: "0", completion: "0", [key]: "0.01" }), key);
    assert.equal(uncoveredPrice({ prompt: "0", completion: "0", [key]: "0" }), undefined);
  }
  assert.equal(uncoveredPrice({ prompt: "0", completion: "0", discount: -0.2 }), "discount");
  assert.equal(uncoveredPrice({ prompt: "0", completion: "0", discount: 0 }), undefined);
  assert.equal(
    uncoveredPrice({ prompt: "0", completion: "0", overrides: [{ other_fee: "1" }] }),
    "other_fee",
  );
  assert.equal(uncoveredPrice({ prompt: "0", completion: "0", odd: "n/a" }), "odd");
});

test("one web search unit is in the worst case and in the cost from tokens", () => {
  const messages = [{ role: "user" as const, content: "" }];
  const searched = model({ prompt: "0", completion: "0", web_search: "0.004" });
  assert.equal(worstCaseMicroUsd(searched, messages, 1), 4000);
  assert.equal(usageMicroUsd(searched, { inputTokens: 0, outputTokens: 0, cachedTokens: 0 }), 4000);
});

test("the cache read price is in the input rate", () => {
  const messages = [{ role: "user" as const, content: "" }];
  assert.equal(
    worstCaseMicroUsd(
      model({ prompt: "0", completion: "0", input_cache_read: "0.000001" }),
      messages,
      1,
    ),
    32,
  );
});
