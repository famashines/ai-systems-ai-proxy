# Changelog

## Unreleased

- Fixed: a model with a `web_search` price is no longer refused. The worst case and the cost from tokens add one search unit for each attempt. Before, the OpenRouter catalog listed `web_search` for normal models, so every call failed with `invalid`.
- Contracts v0.4.1.
- Second durable consumer `ai-proxy-cancel` on `ORCHESTRATION`, filter
  `orchestration.CallCancelRequested` (task M3-11). A cancel is stored in
  `cancels` (TTL 45 days) and aborts the running call in this pod. A call
  in another pod reads `cancels` at each heartbeat (10 s). The call ends
  with `CallFailed` `cancelled`, no `nextModel`, at the real cost (usage,
  the reserved amount when the request was sent, or 0). A `CallRequested`
  after its cancel gets `CallFailed` `cancelled`, cost 0, no reserve.
- The final `CallCompleted` or `CallFailed` has `callInputTokens`,
  `callOutputTokens` and `callCostMicroUsd`, the sum of all attempts in
  `attempts`. An attempt with a `nextModel` has none.
- A signal that is aborted before the provider call sends no request and
  books 0.
- The gitops grants for `ai-proxy-cancel` must merge before this release.

- A `CallRequested` older than 15 minutes gives `CallFailed` `timeout`
  with no `nextModel`, no reservation and no provider call (task M2-18).
  A stored reservation of that attempt is settled with 0 first.
- The interrupted outcome is booked only when the stop aborts the call. A
  provider failure at the same time as a stop keeps its class, so an
  `invalid` does not go to the next model.

## 0.1.0

- Durable consumer `ai-proxy` on `ORCHESTRATION`, filter
  `orchestration.CallRequested`. Events `llm.CallStarted`,
  `llm.CallCompleted`, `llm.CallFailed` on the stream `call-<callId>`, outbox
  relay to `LLM`.
- Provider port and OpenRouter adapter: SSE streaming, mid-stream errors,
  error classes `rate_limit`, `timeout`, `provider`, `invalid`,
  `budget_refused`, privacy flags (`data_collection: "deny"`, `zdr`),
  `service_tier: "default"` and `max_price` at the catalog price.
- Fallback chain over `models`: a new attempt and a new reservation for
  each model, only after a retryable failure.
- Budget: worst case from the catalog prices and `maxTokens`, the
  `maxCostMicroUsd` limit, `rpc.budget.reserve` with a stored
  `reservationId` for each attempt, fail closed, `rpc.budget.settle` with
  the real cost.
- Token deltas on core NATS `delta.<taskId>.<callId>`.
- Call bodies in `call_bodies` (90 days), no secrets.
- Model catalog from `GET /api/v1/models`, cached in `models`, 6 hours.
- Replay API `GET /internal/events`, `/health`, `/ready`, `/metrics`.
- Mock OpenRouter server for tests and the local system.
- Container image, CI and release workflow from the orchestration template.
- Review fixes: no new call while the pod stops; a sent request counts as
  committed (the reserved amount is booked when no usage comes); a settle
  of 0 after a reserve with no answer; `maxCostMicroUsd` minus the cost of
  earlier attempts; a price key that the cost check does not cover makes
  the model `invalid`; a failed settle throws and is repeated from the
  body; 3 tries for the body save; a lease before a crashed attempt is
  resumed (an interrupted body at a stop, a heartbeat each 10 s for a
  crash); `max_price` stored with the attempt; `web_search`,
  `image_output`, `image_token` and a negative `discount` are not covered; 404 is `provider`; size
  limits for the error body (64 KiB) and the stored text (1 MiB); deltas
  split on code points; `OPENROUTER_BASE_URL` must be `https` (or `http`
  for `localhost`).
