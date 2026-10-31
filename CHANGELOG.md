# Changelog

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
