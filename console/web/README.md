# TideMQ Console Web

Browser UI shared by the built-in Combined Console and the independent stateless Console role. The Rust process serves the built assets and the same-origin API; Node.js is used only during development and builds.

## Development

Use Node.js 22.22+, 24.x or 26+ and npm. These versions satisfy the locked React Router and Vitest engines. Install the locked dependencies with `npm ci`, then run `npm run dev`. Vite binds to `127.0.0.1:5173` and proxies `/api` to `http://127.0.0.1:8080`.

## Validation and delivery

- `npm run typecheck`
- `npm run lint`
- `npm run test:coverage`
- `npx playwright install --with-deps chromium`
- `npm run test:e2e -- --project=chromium` (after building)
- `npm run build`

Build output is `dist/`, including `.vite/manifest.json`. Deliver the entire directory with the matching TideMQ binary. The Rust static server uses the manifest as its asset allowlist. No separately deployed Node.js service is required.

## Runtime behavior

The five primary routes provide scoped overviews, Topic configuration and message inspection, subscriptions and consumer progress, principals and grants, and read-only component and node observations. Producers are listed within their Topic. English and Chinese are available, following the browser language by default. The application uses real same-origin APIs; it contains no production mock data or artificial success paths.

`src/api/types.ts` validates internal page responses. The handwritten fetch client handles sessions, CSRF and bounded pagination. No OpenAPI, published API contract or generated client is used. An HTTP 501 response remains an explicit unavailable state and never becomes a successful login or write.

Business writes preserve a UUID v7 operation ID and result link before the first request. Lost replies are recovered by querying the original result; writes are not automatically retried. Grant replacement requires a server preview and its revision. Newly issued token secrets appear only in the first response and are discarded when leaving the result page. Result queries cannot retrieve them.

Tokens and sensitive filters stay in memory, not URLs, browser storage or query keys. The only stored preference is the UI language. Queries use same-origin fetch with no shared cache; authentication and forbidden-resource failures clear protected cache scopes and invalidate late responses. Tests inject responses only in test fixtures.

Explicit sign-in and sign-out notify other open Console tabs through a BroadcastChannel containing only a login/logout marker. Other tabs immediately discard protected data and one-time secrets. A successful sign-in triggers a new identity check; authentication failures do not create notification loops.

Message browsing uses bounded per-Shard offset windows and exact byte filters. Truncated previews are marked separately from original byte counts. Read-only observations do not join consumer groups, renew leases or advance ACK progress. Component trend charts accumulate only actual samples received on the current page, with no synthetic history or zero filling.

Components in `src/components/ui` were added from the official shadcn/ui Radix registry. Registry CSS has been ejected into `src/index.css` so the CLI is not a runtime/build dependency. TanStack Table uses the documented v8 adapter; Query uses v5.

## Frontend quality gates

Vitest 5 with V8 includes every application TypeScript file under `src/`, including lazy pages, UI components and the entry point. Only tests and declaration files are excluded. It combines the existing unit/SSR contract tests (`npm run test:unit`) with React Testing Library, user-event, jsdom and MSW 3 DOM/network integration tests (`npm run test:integration`). The integration tests keep the real request client, Query cache, session provider and operation store. Unhandled MSW network frames fail. jsdom supplies only the missing dialog layout methods; native keyboard and focus behavior runs in real browsers.

`npm run test:coverage` enforces lines/statements/functions >=80% and branches >=70% across all application source. Each authentication/session/request module and permission/write recovery module also requires lines >=90% and branches >=80%. Thresholds are explicit in `vitest.config.ts`; untested UI stays in the denominator. HTML, LCOV, full Istanbul JSON and JSON summary are written to `coverage/unit-integration/`. `COVERAGE_DIR` can select a separate evidence directory; use a dedicated output folder because Vitest cleans it before a run. Browser results are never merged into this source coverage percentage.

`npm run test:e2e` runs the separately labelled `e2e/mock-network/` suite against the production build served by Vite preview. The explicit `mockNetwork` Playwright fixture intercepts only API traffic; this suite proves browser UI behavior and does not prove Rust/backend correctness. Projects are `chromium`, `firefox` and `webkit`. PR CI selects Chromium; main CI selects all three. Run `npx playwright install --with-deps chromium firefox webkit` to provision matching browsers. The Playwright version is pinned so local and CI browser revisions match. Reports go to `playwright-report/mock-network/`; failures retain screenshots, video and traces under `test-results/mock-network/`. Pass `--trace=on` to preserve successful synthetic-fixture traces too.

CI retains one diagnostic retry and fails on flaky results. Investigate and fix the first failure before merging. `node --test tests/ci/*.test.mjs` runs a controlled Playwright probe to verify that passing only after a retry still blocks CI; this gate test does not launch a browser or backend.

Covered interactions include login rejection/logout/session expiry, navigation and locale, offset-bounded message scans and private exact filters, permission redaction and cache invalidation, server-version grant previews, input validation, stable write operation IDs after lost replies, conflicts, result recovery, pagination, stale/unavailable reads and native dialog keyboard/focus behavior.

## Live Rust backend browser smoke

`npm run test:e2e:live -- --project=chromium` invokes `../../scripts/ci/console-browser-live.mjs` after the Rust binary and web assets have been built. The runner owns either one Combined process with its built-in HTTPS Console or separated RF1 processes with an independent HTTPS Console. Select `TIDEMQ_LIVE_TOPOLOGY=combined` (default) or `separated-rf1`; CI covers both. It generates disposable configuration and private TLS material, verifies the native HTTPS endpoint against the generated CA, and cleans up every owned process and private file. Set `TIDEMQ_BINARY` to an absolute TideMQ executable and optionally `TIDEMQ_WEB_DIR` to another built web worktree.

The runner supplies `TIDEMQ_LIVE_BASE_URL` and `TIDEMQ_LIVE_TOKEN_FILE` to `playwright.live.config.ts`. `e2e/live/` uses no request routes, API mocks or service worker. It checks real login, Secure/HttpOnly cookies, authorized directory navigation, reload, logout and a credential write followed by queries of the original result. The fixture identity must be able to issue a credential for its current principal. The one-minute test credential belongs only to the disposable cluster.

Live tests read the private token file only to fill the password input. The runner forces the line reporter and disables tracing; HTML steps can preserve fill values even without tracing. Browser output stays in the disposable private directory. The runner redacts the known bootstrap token across stdout/stderr chunks before writing logs, and retains only those logs, binary identity and a cleanup receipt. Nonzero or signal exits fail the cleanup gate; cleanup still attempts every owned process and private directory. Missing live fixture variables are an error, never a silent skipped pass.
