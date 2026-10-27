# TideMQ Console Web

Browser UI for the stateless TideMQ Console role. The Rust process serves the built assets and the same-origin API; Node.js is used only during development and builds.

## Development

Use Node.js 22.22+, 24.x or 26+ and npm. These versions satisfy the locked React Router and Vitest engines. Install the locked dependencies with `npm ci`, then run `npm run dev`. Vite binds to `127.0.0.1:5173` and proxies `/api` to `http://127.0.0.1:8080`.

## Validation and delivery

- `npm run typecheck`
- `npm run lint`
- `npm test`
- `npm run build`

Build output is `dist/`, including `.vite/manifest.json`. Deliver the entire directory with the matching TideMQ binary. The Rust static server uses the manifest as its asset allowlist. No separately deployed Node.js service is required.

## Runtime behavior

The five primary routes provide scoped overviews, Topic configuration and message inspection, subscriptions and consumer progress, principals and grants, and read-only component and node observations. Producers are listed within their Topic. English and Chinese are available, following the browser language by default. The application uses real same-origin APIs; it contains no production mock data or artificial success paths.

`src/api/types.ts` validates internal page responses. The handwritten fetch client handles sessions, CSRF and bounded pagination. No OpenAPI, published API contract or generated client is used. An HTTP 501 response remains an explicit unavailable state and never becomes a successful login or write.

Business writes preserve a UUID v7 operation ID and result link before the first request. Lost replies are recovered by querying the original result; writes are not automatically retried. Grant replacement requires a server preview and its revision. Newly issued token secrets appear only in the first response and are discarded when leaving the result page. Result queries cannot retrieve them.

Tokens and sensitive filters stay in memory, not URLs, browser storage or query keys. The only stored preference is the UI language. Queries use same-origin fetch with no shared cache; authentication and forbidden-resource failures clear protected cache scopes and invalidate late responses. Tests inject responses only inside test files.

Explicit sign-in and sign-out notify other open Console tabs through a BroadcastChannel containing only a login/logout marker. Other tabs immediately discard protected data and one-time secrets. A successful sign-in triggers a new identity check; authentication failures do not create notification loops.

Message browsing uses bounded per-Shard offset windows and exact byte filters. Truncated previews are marked separately from original byte counts. Read-only observations do not join consumer groups, renew leases or advance ACK progress. Component trend charts accumulate only actual samples received on the current page, with no synthetic history or zero filling.

Components in `src/components/ui` were added from the official shadcn/ui Radix registry. Registry CSS has been ejected into `src/index.css` so the CLI is not a runtime/build dependency. TanStack Table uses the documented v8 adapter; Query uses v5.
