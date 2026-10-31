# ai-systems-ai-proxy

The Model Access service of ai-systems. It reads `orchestration.CallRequested`
from the stream `ORCHESTRATION`, reserves the worst-case cost with the budget
service, calls OpenRouter with streaming, sends each token delta to core
NATS, settles the real cost, and appends `llm.CallStarted`,
`llm.CallCompleted` and `llm.CallFailed` to the stream `call-<callId>` in
MongoDB. The outbox relay publishes them to the stream `LLM`. The service is
internal and has no Ingress.

```
orchestration --orchestration.CallRequested (ORCHESTRATION)--> ai-proxy
ai-proxy, for each attempt (models[0], then the next model on a retryable failure):
  catalog price --> worst case --> rpc.budget.reserve --> llm.CallStarted
  --> POST /chat/completions (SSE) --> delta.<taskId>.<callId> (core NATS)
  --> call body (MongoDB) --> rpc.budget.settle
  --> llm.CallCompleted, or llm.CallFailed (nextModel when one is left)
MongoDB ai_proxy: events, call_bodies, attempts, models --outbox--> LLM
projections --GET /internal/events--> ai-proxy (rebuild only)
```

Architecture and reasons: `ai-systems-docs` (`ARCHITECTURE.md` 3, 6, 7,
ADR 0014, 0015, 0019, 0023, 0025, `research/openrouter.md`). Contracts:
`@famashines/ai-systems-contracts` v0.3.0, README sections "LLM calls",
"Budget rpc" and "Deltas".

## One call

| Step        | Rule                                                                                                                                                                                                                                                                                                                                                                                         |
| ----------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Catalog     | The price of the model comes from the catalog cache. A model that is not in the catalog gives `CallFailed` `invalid`. No catalog at all (never fetched) gives `provider`. Both end the call.                                                                                                                                                                                                 |
| Worst case  | `request + input bound x max(prompt, input_cache_write, input_cache_write_1h) + (maxTokens + 5%) x max(completion, internal_reasoning)`, each price the highest of the base price and all `pricing.overrides`. The input bound is the UTF-8 bytes of the messages plus 16 for each message and 16 more, at most the context length. Exact integer math in atto-USD, rounded up to micro-USD. |
| Call limit  | When `maxCostMicroUsd` is set and the worst case is above it: `CallFailed` `budget_refused`, no reservation, no provider call.                                                                                                                                                                                                                                                               |
| Reservation | The `reservationId`, the `bodyId` and the amount of each (`callId`, `attempt`) are stored in `attempts` before the reserve. A redelivery uses the same values, so the reserve is idempotent. `ttlMs` is `maxDurationMs` plus 60 s, at most 1 hour.                                                                                                                                           |
| Refusal     | A refusal, an error or no answer in 2 s gives `CallFailed` `budget_refused` (fail closed, ADR 0015). No provider call.                                                                                                                                                                                                                                                                       |
| Start       | `llm.CallStarted` is appended with the expected version before the provider call, so only one runner calls the provider for an attempt.                                                                                                                                                                                                                                                      |
| Call        | One model for each attempt. The call stops after `maxDurationMs` (`timeout`).                                                                                                                                                                                                                                                                                                                |
| Deltas      | Core NATS `delta.<taskId>.<callId>`, body `{ attempt, i, text }`. `i` starts at 0 for each attempt. NUL is removed. A text above 16,384 characters is split.                                                                                                                                                                                                                                 |
| Body        | The request body and the response (text, finish reason, usage, error, generation id, status) are stored in `call_bodies` with the outcome, before the settle. No headers, so no API key. Kept 90 days.                                                                                                                                                                                       |
| Settle      | The real cost. A settle that fails is logged; the reservation then expires and the budget books the full amount.                                                                                                                                                                                                                                                                             |
| End         | `CallCompleted` (also with empty text; orchestration makes `RunFailed` `empty_output`), or `CallFailed`. The last event and the `processed` marker are in one transaction.                                                                                                                                                                                                                   |

Cost of an attempt:

| Case                                            | Cost                                                                  |
| ----------------------------------------------- | --------------------------------------------------------------------- |
| `usage.cost` is in the stream                   | `Math.round(cost x 1,000,000)`                                        |
| usage with no cost                              | tokens x catalog prices (input and output rates as in the worst case) |
| no usage, the response was committed (HTTP 200) | the reserved amount (the real cost is not known)                      |
| no usage, an HTTP error before the stream       | 0                                                                     |

`CallCompleted.text` has no NUL and at most 100,000 characters. The full
text is in the call body.

## Error classes

| Class            | From                                                                                                                                          | Next model |
| ---------------- | --------------------------------------------------------------------------------------------------------------------------------------------- | ---------- |
| `rate_limit`     | 429, `rate_limit_exceeded`, 402 with `limit_source` `openrouter_in_flight_budget`                                                             | yes        |
| `timeout`        | 408, 504, 524, `timeout`, no end in `maxDurationMs`                                                                                           | yes        |
| `provider`       | 5xx, 529, `provider_unavailable`, `provider_overloaded`, `server`, a network error, a bad chunk, a stream with no end, a crash in the attempt | yes        |
| `invalid`        | other 4xx (400, 401, 403, 404, 413, 422 ...), a model that is not in the catalog                                                              | no         |
| `budget_refused` | budget refusal or no answer, the call limit, 402 (credits, key limit), 403 with a `limit_source`                                              | no         |

The `error.metadata.error_type` wins over the HTTP status. A mid-stream
error (HTTP 200, `finish_reason: "error"`) uses `error.code` (a number or a
string). The `reason` has only the status, the error type and the class,
never the provider text. The provider text is in the call body.

Orchestration retries the full call with backoff (ARCHITECTURE 8.2). The
fallback chain runs inside one try.

## Idempotency

```
redelivery of CallRequested
  last event is final                        --> nothing
  attempt n started, body stored             --> outcome from the body, settle, end event; no provider call
  attempt n started, no body (crash in call) --> CallFailed provider, cost = reserved amount, next model
  attempt n not started                      --> same reservationId and amount, reserve again
```

## OpenRouter facts used (read 2026-10-31)

From `ai-systems-docs/research/openrouter.md` (M2-01), which has the sources:

- `POST https://openrouter.ai/api/v1/chat/completions` with
  `Authorization: Bearer <key>`, `stream: true`. SSE `data:` lines, comment
  lines start with `:`, the end is `data: [DONE]`. The last data chunk
  carries `usage` and repeats `finish_reason`.
- `usage.prompt_tokens`, `completion_tokens`,
  `prompt_tokens_details.cached_tokens`, `cost` (USD, nullable).
- Errors before the stream: HTTP status and
  `{ error: { code, message, metadata: { error_type, limit_source } } }`.
  Errors in the stream: HTTP 200, a chunk with `error` and
  `finish_reason: "error"`, also as the first and only event.
- `GET /api/v1/models`: `data[].id`, `context_length`, `pricing` (strings,
  USD for each token; `"-1"` on routers, skipped), `pricing.overrides`.
- Request fields sent: `model`, `messages`, `max_tokens`, `stream`,
  `service_tier: "default"` (keeps priority endpoints out of the pool),
  `provider.data_collection: "deny"`, `provider.zdr` (env
  `OPENROUTER_ZDR`, default `true`), and `provider.max_price` (USD for each
  million tokens) at the catalog price, so the routed endpoint cannot cost
  more than the reserved worst case.
- Not sent: `HTTP-Referer` and `X-OpenRouter-Title`. They make an app page
  in the public rankings. No plugins, no tools, no `models` array (each
  model is its own attempt with its own reservation).

## API

| Method | Path                                           | Does                                                                                                        |
| ------ | ---------------------------------------------- | ----------------------------------------------------------------------------------------------------------- |
| GET    | `/internal/events?afterPosition=<n>&limit=<m>` | Replay API: all events in position order (`limit` 1 to 1000)                                                |
| GET    | `/health`                                      | Liveness: 200 while the process runs                                                                        |
| GET    | `/ready`                                       | 200 when MongoDB answers, the migrations are done and the consumer exists; else 503 with the failing checks |
| GET    | `/metrics`                                     | Prometheus metrics                                                                                          |

## Configuration

Environment variables (`.env.example` has all of them):

| Variable                                                            | Default                        | Content                                                           |
| ------------------------------------------------------------------- | ------------------------------ | ----------------------------------------------------------------- |
| `MONGODB_URI`                                                       | required                       | MongoDB connection string (replica set)                           |
| `MONGODB_DATABASE`                                                  | `ai_proxy`                     | Database name                                                     |
| `NATS_SERVERS`                                                      | required                       | NATS URLs, comma-separated                                        |
| `NATS_USER`                                                         | required                       | `ai-systems-ai-proxy` on the cluster                              |
| `NATS_PASSWORD`                                                     | required                       | From the SealedSecret                                             |
| `OPENROUTER_API_KEY`                                                | required                       | Secret `ai-systems-ai-proxy-openrouter`, key `OPENROUTER_API_KEY` |
| `OPENROUTER_BASE_URL`                                               | `https://openrouter.ai/api/v1` | The mock server in tests and in the local system                  |
| `OPENROUTER_ZDR`                                                    | `true`                         | `provider.zdr` on each request (`true` or `false`)                |
| `PORT`, `LOG_LEVEL`, `RELEASE`                                      | `8080`, `info`, `local`        | As in the kit                                                     |
| `OTEL_EXPORTER_OTLP_ENDPOINT`, `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` | not set                        | Traces go over OTLP only when one is set                          |

The kit service name is `ai-proxy`: the NATS inbox prefix is
`_INBOX_ai-proxy` and the durable consumer is `ai-proxy` on `ORCHESTRATION`
with the filter `orchestration.CallRequested` (ADR 0023). The grants of the
user `ai-systems-ai-proxy` in gitops are complete for this service.

## Mock OpenRouter

`test/mock-openrouter.ts` is an HTTP server that acts as OpenRouter: the
models API and SSE streams with comment lines, small TCP pieces, the usage
chunk, mid-stream errors, HTTP errors and calls that hang. Tests script it
for each model. `npm run mock:openrouter` starts it on port 8090 (env
`PORT`); with no script it streams "Mock answer." Set
`OPENROUTER_BASE_URL=http://<host>:8090/api/v1`. No test calls the real
OpenRouter.

## Known limits

- The kit consumer handles the messages of one fetch one after the other,
  so one pod runs one call at a time, up to `maxDurationMs`. This is
  enough for M2 (one agent). Upgrade path: a concurrency option in the
  kit consumer, or more pods.
- The cost of an attempt that fails after HTTP 200 with no usage chunk is
  not known, so the reserved amount is booked. Upgrade path:
  `GET /api/v1/generation?id=` (its delay is not documented).
- The input token bound is the UTF-8 byte count (each token is at least
  one byte), capped by the context length. It is high for normal text.
- The catalog is refreshed when it is used and older than 6 hours (and at
  most one try each minute after a failure). A stale price is safe because
  `provider.max_price` refuses a more expensive endpoint.
- During a shutdown, a running provider call is stopped and the message
  comes again; the attempt is then booked at the reserved amount.

## Commands

See `CLAUDE.md`.
