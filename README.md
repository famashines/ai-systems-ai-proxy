# ai-systems-ai-proxy

The Model Access service of ai-systems. It reads `orchestration.CallRequested`
and `orchestration.CallCancelRequested` from the stream `ORCHESTRATION`
(two durable consumers), reserves the worst-case cost with the budget
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
orchestration --orchestration.CallCancelRequested (ORCHESTRATION)--> ai-proxy
  --> cancels (MongoDB) --> abort the running call --> llm.CallFailed cancelled
MongoDB ai_proxy: events, call_bodies, attempts, cancels, models --outbox--> LLM
projections --GET /internal/events--> ai-proxy (rebuild only)
api-gateway --GET /internal/models--> ai-proxy --> user models cache (GET /api/v1/models/user)
```

Architecture and reasons: `ai-systems-docs` (`ARCHITECTURE.md` 3, 6, 7,
ADR 0014, 0015, 0019, 0023, 0025, `research/openrouter.md`). Contracts:
`@famashines/ai-systems-contracts` v0.5.0, README sections "LLM calls",
"Tasks and plans", "Budget rpc", "Deltas" and the model list rules.

## One call

A stopping pod starts no new call: the handler checks the stop at its
start and again before the reserve.

A request older than 15 minutes (its envelope `occurredAt`, the stale
rule of orchestration) starts no new attempt: no new reservation and no
provider call. When the attempt has a stored reservation (reserved, but
no `CallStarted`), the handler settles it with 0 first. It gives
`CallFailed` `timeout` with no `nextModel`, so the call ends (a missing
`nextModel` ends a call for each class). Orchestration maps `timeout` to
`unavailable`, the same code as its own stale rule, and logs the event as
a late call event when the task is closed.

| Step        | Rule                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
| ----------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Catalog     | The price of the model comes from the catalog cache. A model that is not in the catalog gives `CallFailed` `invalid`. No catalog at all (never fetched) gives `provider`. Both end the call.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| Price keys  | A model with a price above 0 for a key that the worst case does not cover gives `CallFailed` `invalid` and ends the call. Covered: `prompt`, `completion`, `request`, `internal_reasoning`, `input_cache_write`, `input_cache_write_1h`, `input_cache_read`, `web_search` (one search unit for each attempt, added to the request price, because a search-native model charges it for each request). Not charged, because a request has only text input, no plugins and no tools: `image`, `audio`, `audio_output`, `input_audio_cache`, and the override conditions. `discount` is fine at 0 or more; a negative `discount` is not covered. `image_output` and `image_token` above 0 are not covered.      |
| Effort      | A `CallRequested` with an `effort` gets `CallFailed` `invalid`, cost 0, no reservation and no provider call, when the effort is not in the `efforts` of the catalog model, or when the effort is not `none` and `maxTokens` is 1024 or less (the provider needs `max_tokens` above the reasoning budget). Else the request has `reasoning: { effort }`. No `effort` sends no `reasoning`. The effort is in the stored request body, not in an `llm.*` event.                                                                                                                                                                                                                                                |
| Worst case  | `request + input bound x max(prompt, input_cache_write, input_cache_write_1h, input_cache_read) + output bound x max(completion, internal_reasoning)`, each price the highest of the base price and all `pricing.overrides`. The input bound is the UTF-8 bytes of the messages plus 16 for each message and 16 more, at most the context length. The output bound is `max(maxTokens, 16)` plus 5%. Exact integer math in atto-USD, rounded up to micro-USD. Reasoning tokens bill as output, and on most providers `max_tokens` includes them, so the output bound covers them. When the usage has more output tokens than the output bound, the handler logs one warning; the settle books the real cost. |
| Call limit  | When `maxCostMicroUsd` is set (the rest of the task allowance), the limit for an attempt is that value minus the cost booked by the earlier attempts of this call. A worst case above it gives `CallFailed` `budget_refused`, no reservation, no provider call.                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| Reservation | The `reservationId`, the `bodyId`, the amount and the `max_price` of each (`callId`, `attempt`) are stored in `attempts` before the reserve. A redelivery uses the same values (no new catalog price), so the reserve is idempotent. The call time is `maxDurationMs`, at most 3,540,000 ms; `ttlMs` is the call time plus 60 s.                                                                                                                                                                                                                                                                                                                                                                            |
| Refusal     | A refusal gives `CallFailed` `budget_refused`. An error or no answer in 2 s also gives `budget_refused` (fail closed, ADR 0015), after a settle of 0 with the same `reservationId` that frees a hold that the budget made with no answer. If that settle fails, it is logged, and the budget books the hold in full when it expires. No provider call.                                                                                                                                                                                                                                                                                                                                                      |
| Start       | `llm.CallStarted` is appended with the expected version before the provider call, so only one runner calls the provider for an attempt.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |
| Call        | One model for each attempt. The call stops after the call time (`timeout`).                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
| Deltas      | Core NATS `delta.<taskId>.<callId>`, body `{ attempt, i, text }`. `i` starts at 0 for each attempt. NUL is removed. A text above 16,384 characters is split on a code point boundary.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| Body        | The request body and the response (text, finish reason, usage, error, generation id, status) are stored in `call_bodies` with the outcome, before the settle. No headers, so no API key. The error body is read up to 64 KiB. The stored text stops at 1 MiB with `truncated: true`. A failed save is tried 3 times, then the handler throws. Kept 90 days.                                                                                                                                                                                                                                                                                                                                                 |
| Settle      | The real cost. A settle that fails throws before the end event. The next delivery takes the outcome from the body and settles again; `already_settled` makes this safe.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |
| End         | `CallCompleted` (also with empty text; orchestration makes `RunFailed` `empty_output`), or `CallFailed`. The last event and the `processed` marker are in one transaction.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| Totals      | The tokens and the cost of each attempt are stored on its `attempts` document after the settle. The final event (`CallCompleted`, or `CallFailed` with no `nextModel`) has `callInputTokens`, `callOutputTokens` and `callCostMicroUsd`: the sum of all `attempts` documents of the call. An attempt with no document (refused before the reserve, not in the catalog, cancelled before the start) costs 0. An event with a `nextModel` has no totals.                                                                                                                                                                                                                                                      |

Cost of an attempt:

| Case                                                                                      | Cost                                                                                    |
| ----------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------- |
| `usage.cost` is in the stream                                                             | `Math.round(cost x 1,000,000)`                                                          |
| usage with no cost                                                                        | tokens x catalog prices (input and output rates as in the worst case)                   |
| no usage, the request was sent (HTTP 200, a timeout or a socket error after the request)  | the reserved amount (the real cost is not known; some providers bill the full response) |
| no usage, an HTTP error status, or a connect error before the request (refused, DNS, TLS) | 0                                                                                       |

`CallCompleted.text` has no NUL and at most 100,000 characters. A longer
text is cut, and then `finishReason` is `length`. The full text is in the
call body.

`kind` and `subtaskId` of `CallRequested` are not used. No `llm.*` event
has these fields, so ai-proxy copies them nowhere.

## Cancel

The durable consumer `ai-proxy-cancel` on `ORCHESTRATION` reads only
`orchestration.CallCancelRequested`. The two consumers keep no order
between them, so a cancel can come before, during or after its call.

```
CallCancelRequested --> store callId in cancels --> abort the call, if it runs in this pod
CallRequested, before each attempt --> callId in cancels?
  yes --> settle a stored reservation with 0 --> CallFailed cancelled, cost 0, no reserve
before the provider call, and at each heartbeat (10 s) --> callId in cancels? yes --> abort
```

| Case                                                  | Result                                                                                                                                                                                                                                                                                            |
| ----------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| The call runs in this pod                             | The cancel handler aborts it at once. The attempt ends with `CallFailed` `cancelled`, no `nextModel`. The cost follows the cost table: `usage` when the stream had it, the reserved amount when the request was sent, 0 when no request was sent. The settle books that cost.                     |
| The call runs in another pod                          | The running pod reads `cancels` once (awaited) after `CallStarted` and before the provider call, and then at each heartbeat (each 10 s, the same timer as `heartbeatAt`). When it finds the cancel, it aborts the call. Then the same as above. A call that has started can run up to 10 s more.  |
| The call has not come yet                             | The cancel stays in `cancels`. The later `CallRequested` gets `CallFailed` `cancelled`, cost 0, no reserve and no provider call.                                                                                                                                                                  |
| The call has ended                                    | Nothing. A redelivered `CallRequested` reads the final event first.                                                                                                                                                                                                                               |
| Between two attempts, or after a crash                | A retryable failure of a cancelled call becomes `cancelled` with its own cost and no `nextModel`. A crashed attempt (heartbeat older than 30 s) is booked at the reserved amount as `cancelled`. A cancel that comes after the end event of an attempt stops the next attempt before its reserve. |
| A cancel during the reserve (this pod or another pod) | `CallStarted` is appended, the provider gets no request, the settle is 0, and `CallFailed` `cancelled` has cost 0.                                                                                                                                                                                |
| A stop of the pod at the same time                    | As for a stop, the handler saves the body and throws, with the outcome `cancelled` at the reserved amount. The next delivery ends the call as `cancelled` from the body.                                                                                                                          |

A cancel is stored as `{ _id: callId, createdAt }` in `cancels`. The
handler is idempotent (upsert) and marks nothing as processed. The TTL
is 45 days, the same as `attempts` and `processed`, so all state of a call
expires together. A call needs its cancel for at most about 75 minutes
(15 minutes until the stale rule stops new attempts, plus 59 minutes of
call time), so 45 days also answers a DLQ replay of an old
`CallRequested` with `cancelled`. The documents are small.

## Error classes

| Class            | From                                                                                                                                                                                                                          | Next model |
| ---------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------- |
| `rate_limit`     | 429, `rate_limit_exceeded`, 402 with `limit_source` `openrouter_in_flight_budget`                                                                                                                                             | yes        |
| `timeout`        | 408, 504, 524, `timeout`, no end in the call time; a request older than 15 minutes (no next model)                                                                                                                            | yes        |
| `provider`       | 404 (also "no endpoint" for the `zdr`, `data_collection` or `max_price` rules), 5xx, 529, `provider_unavailable`, `provider_overloaded`, `server`, a network error, a bad chunk, a stream with no end, a crash in the attempt | yes        |
| `invalid`        | other 4xx (400, 401, 403, 413, 422 ...), a model that is not in the catalog, a price key that is not covered, an effort that the model does not have, an effort other than `none` with `maxTokens` 1024 or less               | no         |
| `budget_refused` | budget refusal or no answer, the call limit, 402 (credits, key limit), 403 with a `limit_source`                                                                                                                              | no         |
| `cancelled`      | `orchestration.CallCancelRequested` for the call (see Cancel)                                                                                                                                                                 | no         |

The `error.metadata.error_type` wins over the HTTP status. A mid-stream
error (HTTP 200, `finish_reason: "error"`) uses `error.code` (a number or a
string). The `reason` has only the status, the error type and the class,
never the provider text. The provider text is in the call body.

Orchestration retries the full call with backoff (ARCHITECTURE 8.2). The
fallback chain runs inside one try.

## Idempotency

```
redelivery of CallRequested
  last event is final                         --> nothing
  callId in cancels, attempt n not started    --> settle a stored reservation with 0, CallFailed cancelled, no provider call
  attempt n started, body stored              --> outcome from the body, settle, end event; no provider call
  attempt n started, no body, heartbeat fresh --> wait until it is 30 s old, then read the stream again
  attempt n started, no body, heartbeat old   --> CallFailed provider, cost = reserved amount, next model
  attempt n not started                       --> same reservationId, amount and max_price, reserve again
  attempt n not started, request too old      --> settle a stored reservation with 0, CallFailed timeout, no provider call
```

- Stop: when the stop aborts a running call, the handler saves the body
  with an interrupted outcome (the reserved amount) before it throws.
  A provider failure that is not from the abort (for example a 400) keeps
  its class, also when the stop comes at the same time.
  MongoDB is still open, because the consumer closes first. The next
  delivery takes that outcome at once.
- Crash: while a call streams, the handler renews `heartbeatAt` on the
  `attempts` document each 10 s. A redelivery waits only while the
  heartbeat is less than 30 s old (another pod can still run the call),
  then it books the attempt as interrupted.
- The kit consumer has no nak with a chosen delay, so the handler waits
  itself; the kit keeps the message in progress. A stop during the wait
  throws, and the message comes again.

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
- `GET /api/v1/models`: `data[].id`, `name`, `context_length`, `pricing`
  (strings, USD for each token; `"-1"` on routers, skipped),
  `pricing.overrides`, `reasoning` (`supported_efforts`: an array, highest
  first, or `null` for all values; `default_effort`; `mandatory`: `true`
  means `none` is refused).
- `GET /api/v1/models/user` with the key: the same entry shape, only the
  models that the key can use (the provider preferences, privacy settings
  and guardrails of the account). ai-proxy limits the list with
  `MODEL_LIST`, not with a guardrail: a guardrail limits the key, so it
  also blocks the models of the agents (ADR 0031).
- Request fields sent: `model`, `messages`, `max_tokens`, `reasoning`
  (only with an effort), `stream`,
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
| GET    | `/internal/models`                             | The models that the key can use (`ModelListResultV1`), sorted by `name`; 503 when the list is not available |
| GET    | `/health`                                      | Liveness: 200 while the process runs                                                                        |
| GET    | `/ready`                                       | 200 when MongoDB answers, the migrations are done and the consumer exists; else 503 with the failing checks |
| GET    | `/metrics`                                     | Prometheus metrics                                                                                          |

## Model list

`GET /internal/models` gives one item for each model of the user models
list: `id`, `name` (the `id` when OpenRouter has no valid name), `efforts`
and `defaultEffort`. A model with a price key that the cost check does
not cover is not in the list, because a call fails it with `invalid`. The
list is sorted by `name` and has at most 1000 items (the limit of
`ModelListResultV1`); ai-proxy drops the items after the first 1000. 503
when the list is not available, also when MongoDB is down and the pod has
no list in memory. When `MODEL_LIST` is set, the list has only the
models in it, after the price check and before the limit of 1000. A
listed id that is not in the user models list is not in the result. An
id that is not a model id stops the start with a config error. The call
path does not use `MODEL_LIST`: a `CallRequested` can use each model of
the catalog, so the models of the agents (Auto) keep working (ADR 0031).
ai-proxy makes `efforts` from the `reasoning` object:

- `efforts` is always an array, highest effort first.
- `supported_efforts: null` gives all 7 values of `ReasoningEffort`.
- No `reasoning` object, no `supported_efforts`, or a bad `reasoning`
  object gives `[]`. The model stays in the list.
- `mandatory: true` removes `none`.
- A value that is not in `ReasoningEffort` is dropped.
- `defaultEffort` is present only when it is in `efforts` and is not
  `none`.

The call catalog (`GET /models`) uses the same rules, so the effort check
of a call and the list agree.

## Configuration

Environment variables (`.env.example` has all of them):

| Variable                                                            | Default                        | Content                                                              |
| ------------------------------------------------------------------- | ------------------------------ | -------------------------------------------------------------------- |
| `MONGODB_URI`                                                       | required                       | MongoDB connection string (replica set)                              |
| `MONGODB_DATABASE`                                                  | `ai_proxy`                     | Database name                                                        |
| `NATS_SERVERS`                                                      | required                       | NATS URLs, comma-separated                                           |
| `NATS_USER`                                                         | required                       | `ai-systems-ai-proxy` on the cluster                                 |
| `NATS_PASSWORD`                                                     | required                       | From the SealedSecret                                                |
| `OPENROUTER_API_KEY`                                                | required                       | Secret `ai-systems-ai-proxy-openrouter`, key `OPENROUTER_API_KEY`    |
| `OPENROUTER_BASE_URL`                                               | `https://openrouter.ai/api/v1` | `https` only; `http` only for `localhost` and `127.0.0.1` (the mock) |
| `OPENROUTER_ZDR`                                                    | `true`                         | `provider.zdr` on each request (`true` or `false`)                   |
| `MODEL_LIST`                                                        | not set (no filter)            | Model ids, comma-separated, for `/internal/models` (Model list)      |
| `PORT`, `LOG_LEVEL`, `RELEASE`                                      | `8080`, `info`, `local`        | As in the kit                                                        |
| `OTEL_EXPORTER_OTLP_ENDPOINT`, `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` | not set                        | Traces go over OTLP only when one is set                             |

The kit service name is `ai-proxy`: the NATS inbox prefix is
`_INBOX_ai-proxy`. The durable consumers on `ORCHESTRATION` are `ai-proxy`
with the filter `orchestration.CallRequested` and `ai-proxy-cancel` with the
filter `orchestration.CallCancelRequested` (ADR 0023 must change: two
durables for one service). The DLQ subjects are `dlq.ai-proxy.>` and
`dlq.ai-proxy-cancel.>`. The user `ai-systems-ai-proxy` in gitops needs
these grants for the second consumer:

```
dlq.ai-proxy-cancel.>
$JS.API.CONSUMER.CREATE.ORCHESTRATION.ai-proxy-cancel.orchestration.CallCancelRequested
$JS.API.CONSUMER.INFO.ORCHESTRATION.ai-proxy-cancel
$JS.API.CONSUMER.MSG.NEXT.ORCHESTRATION.ai-proxy-cancel
$JS.ACK.ORCHESTRATION.ai-proxy-cancel.>
```

Deploy order: merge these grants before the release of this version.
Without them the second consumer is not made and `/ready` stays 503, so
the rollout stops. The grants do nothing for an older version.

## Mock OpenRouter

`test/mock-openrouter.ts` is an HTTP server that acts as OpenRouter: the
models API and SSE streams with comment lines, small TCP pieces, the usage
chunk, mid-stream errors, HTTP errors and calls that hang. Tests script it
for each model. `npm run mock:openrouter` starts it on port 8090 (env
`PORT`); with no script it streams "Mock answer." Set
`OPENROUTER_BASE_URL=http://127.0.0.1:8090/api/v1`. No test calls the real
OpenRouter.

## Known limits

- A cancel for a call in another pod waits for the next heartbeat of that
  pod, up to 10 s. The call streams and costs money until then. Upgrade
  path: a shorter poll, or a core NATS subject that all pods read.
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
  `provider.max_price` refuses a more expensive endpoint. The user models
  list has the same rules and its own document. The `models` collection
  has the documents `openrouter-v2` (`GET /models`, for the price and the
  efforts of a call) and `openrouter-user-v1` (`GET /models/user`, for
  `/internal/models`). The old document `openrouter` (no efforts) is not
  read.
- During a shutdown, a running provider call is stopped and the attempt
  is booked at the reserved amount (the real cost is not known).
- After a crash, a redelivery waits up to 30 s for the heartbeat to get
  old.
- A long budget outage (the settle fails for 5 deliveries, about 2.5
  minutes) sends the message to the DLQ. The body and the reservation
  stay, so a DLQ replay settles and ends the call with no new provider
  call. An alert on `dlq.ai-proxy.>` is a follow-up.
- `provider.max_price` is a float in USD for each million tokens. A price
  that is not exact in binary can give a value a little below the
  endpoint price, and then OpenRouter has no endpoint (404, `provider`).
  Check this in the M2-09 smoke test.
- `OPENROUTER_BASE_URL` accepts `http` only for `localhost` and
  `127.0.0.1`. A mock in another container (for example `http://mock:8090`
  in the kit compose system) needs `https` or a change of this rule.

## Commands

See `CLAUDE.md`.
