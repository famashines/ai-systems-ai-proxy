# CLAUDE.md for ai-systems-ai-proxy

Rules for all ai-systems repos: `ai-systems-docs/CLAUDE.md`
(local: `../../docs/CLAUDE.md`). What the service does: `README.md`.

## Commands

| Command                    | Does                                                             |
| -------------------------- | ---------------------------------------------------------------- |
| `npm ci`                   | install from the lock file (builds kit and contracts on install) |
| `npm run check`            | `tsc --noEmit` (TypeScript 7)                                    |
| `npm run lint`             | ESLint (strict type-checked) and Prettier check                  |
| `npm test`                 | unit tests, two coverage gates (see below)                       |
| `npm run verify`           | check, lint and test                                             |
| `npm run test:integration` | `test/integration` with Testcontainers (needs Docker)            |
| `npm run mock:openrouter`  | the mock OpenRouter server on port 8090                          |
| `npm start`                | `node src/main.ts`                                               |

## Layout

```
src
├── domain/cost.ts             prices, worst case, cost from usage, max_price; no I/O
├── domain/call.ts             call state (evolve, next step), outcome, text rules; no I/O
├── application/handle-call.ts the CallRequested and CallCancelRequested handlers
├── adapters/openrouter.ts     the provider port and the OpenRouter adapter (SSE, errors, catalog)
├── adapters/mongo.ts          attempts, call bodies, cancels, catalog cache, own migrations
├── adapters/http.ts           the replay API
└── main.ts                    config, wiring by constructor, start, shutdown
test
├── mock-openrouter.ts         the mock OpenRouter HTTP server
├── unit                       node:test, kit in-memory event store, the mock server
└── integration                MongoDB and NATS in Testcontainers, the mock server
```

## Rules

- No test calls the real OpenRouter. Use `test/mock-openrouter.ts`. Test
  keys are plain words, never a real key format (gitleaks).
- Never store or log the API key or request headers. The call body has
  only the JSON request body and the parsed response.
- Provider text (error messages, output) is untrusted data. It goes into
  the call body, never into a `reason`.

## Coverage gates (ADR 0017)

- `test:domain`: `src/domain`, 90% lines and 85% branches.
- `test:unit`: `src` without `main.ts`, 70% lines.

## Integration tests

The NATS config in the test has the grants of the users
`ai-systems-ai-proxy`, `ai-systems-budget`, `ai-systems-socket-gateway` and
`ai-systems-admin` from gitops (`kubernetes/infrastructure/nats/values.yaml`).
Change them together with gitops. The test budget answers the rpc
subjects with the budget user. Wait with `waitFor`, never with a fixed
sleep. The CI job runs on `ubuntu-22.04` (`mongo:8.0.34` does not start on
the kernel of `ubuntu-24.04`).

## Dependencies

- Kit (v0.3.0) and contracts (v0.5.0) come from git tags. A new tag needs a new
  `allowScripts` entry with the commit of the tag
  (`npm install-scripts approve <package>`).
- `mongodb` and `zod` have the exact versions that the kit pins. Check
  with `npm ls zod mongodb @famashines/ai-systems-contracts` (one copy each).
- TypeScript 6 (`typescript`, for ESLint) and 7 (`typescript-7`, for
  `check`): ADR 0022. Never run `npx tsc`.
