# Flows dashboard

`iterativeflow/dashboard` is a mountable observability UI for an engine —
an overview, a runs list with filters and keyset pagination, and a crons view,
across three pages. Open any run for a detail panel (steps, sleeps, signals,
capped input/output payloads) and act on it: **cancel**, **retry**, or
**deliver a signal** to a run that's waiting on one. The open panel and active
filters live in the URL, so a refresh or a shared link restores your view.

It consumes only the public `Engine` API (`listRuns`, `status`, `health`,
`cancel`, `retry`, `signal`), so it works against any deployment the engine
works against, and adds **no runtime dependencies** to your app: a pre-built
single-page UI is shipped in the package and served as a hashed JS/CSS bundle.
Pass your own `CronSpec[]` as `crons` and the dashboard also lists them and lets
you trigger one on demand — see [Crons](#crons).

![Runs list](./assets/dashboard-runs.png)

```text
createFlowsDashboard({ engine, crons? }).fetch
        │
        ├── GET  <mount>/…           → the app shell (hash-routed SPA)
        ├── GET  <mount>/assets/…    → hashed JS/CSS build assets
        ├── GET  <mount>/api/health  → engine.health()
        ├── GET  <mount>/api/runs    → engine.listRuns(...)  (filters + cursor)
        ├── GET  <mount>/api/runs/:id           → engine.status(runId)
        ├── POST <mount>/api/runs/:id/cancel    → engine.cancel(runId, reason?)
        ├── POST <mount>/api/runs/:id/retry     → engine.retry(runId)
        ├── POST <mount>/api/runs/:id/signal    → engine.signal(runId, name, payload?)
        ├── GET  <mount>/api/crons              → the crons you passed in
        └── POST <mount>/api/crons/:name/run    → calls that cron's run() now
```

## Mount it

`createFlowsDashboard` returns a WHATWG fetch handler — `(req: Request) =>
Promise<Response>` — so it mounts anywhere that speaks fetch. It serves the
UI for any GET that isn't an API route and pins a `<base>` tag to the request
path, so **any mount path works** without configuration.

```ts
import { createFlowsDashboard } from "iterativeflow/dashboard";

const dashboard = createFlowsDashboard({ engine });
// dashboard.fetch: (req: Request) => Promise<Response>
```

Next.js route handler (`app/admin/flows/[[...rest]]/route.ts`):

<!-- doc-check: skip — imports the host app's modules -->

```ts
import { createFlowsDashboard } from "iterativeflow/dashboard";
import { engine } from "@/flows/engine";
import { withAdminAuth } from "@/auth";

const dashboard = createFlowsDashboard({ engine });

export const GET = withAdminAuth((req: Request) => dashboard.fetch(req));
export const POST = withAdminAuth((req: Request) => dashboard.fetch(req));
```

Hono: `app.use("/admin/flows/*", yourAuth).mount("/admin/flows", dashboard.fetch)`.
Plain `node:http`, Express, or anything else request/response-shaped can
bridge to it the same way.

The handler loads its HTML asset through `node:fs`, so it needs a Node.js
runtime (a Vercel/Lambda Node function is fine; an edge/workerd runtime
without `fs` is not).

## Bring your own auth

The dashboard performs **no authentication** — mounting it unprotected
exposes run payloads and cancel/retry to anyone who can reach the route. Put
it behind whatever your app already uses (session middleware, basic auth, a
VPN). Two properties help you hold that line:

- Mutations require a `content-type: application/json` header. Cross-origin
  HTML forms can't send one without a CORS preflight, which closes off CSRF
  when the dashboard sits behind cookie auth. (The dashboard sets no CORS
  headers, so cross-origin `fetch` mutations are not replies you'll serve.)
- Every response is `cache-control: no-store`, so payloads don't linger in
  shared caches.

## A web process can host it without consuming the queue

`createEngine(...)` is inert until `engine.listen()` — constructing an engine
just to pass it to the dashboard does not start a worker. An API/web process
can therefore share the flow definitions, skip `listen()`, and still observe
and manage runs; the resident worker process elsewhere keeps consuming.

Related: the header's health strip reports `engine.health()` **for the
process serving the dashboard**. In the setup above, `worker: false` /
`listen: false` is expected and correct — only `db` speaks for the system as
a whole.

## What the actions do

Cancel, retry, and signal are the engine's own semantics, surfaced from the
run's detail panel:

- **Cancel** (any non-terminal run) — marks the run `canceled`; if the run is
  mid-step in the same process, its `AbortSignal` fires. From another process
  the run stops at its next suspend point.
- **Retry** (failed runs only) — re-enqueues the run: memoized `ok` step
  results are preserved, the `failed_terminal` step row is deleted, the run
  resets to `pending` and replays from the failing step (see
  [replay semantics](./replay-semantics.md)). The dashboard maps the
  `RetryResult` to HTTP: `queued` → 200, `missing` → 404, `not_failed` → 409.
- **Signal** (a run awaiting one) — delivers a named signal with an optional
  JSON payload via `engine.signal(runId, name, payload)`. The dashboard maps
  the `SignalDeliveryResult`: `delivered`/`buffered`/`duplicate` → 200,
  `invalid_payload` → 422, `expired` → 409. See [signals](./signals.md).

## Crons

The engine has no public API to enumerate the crons registered on it, so the
dashboard doesn't try to read them back — pass the same `CronSpec[]` you gave
to `engine.defineCron(...)` as `crons`, and it lists them with a **Run now**
button:

Opening a cron shows its sheet: a **Run now** trigger, the last trigger's
result, and the runs it started — the dashboard lists those by filtering runs
tagged `cron:<name>`, so tag the runs your cron starts to see them here.

![A cron's sheet: the runs it started, tagged cron:<name>, plus a Run now trigger](./assets/dashboard-crons.png)

```ts
import { createFlowsDashboard } from "iterativeflow/dashboard";

const crons = [
  { name: "nightly-sweep", schedule: "0 2 * * *", run: async () => ({ synced: 0 }) },
  // ...whatever you also pass to engine.defineCron
];

for (const cron of crons) engine.defineCron(cron);

const dashboard = createFlowsDashboard({ engine, crons });
```

`GET <mount>/api/crons` returns `name`/`schedule`/`timezone`/`overlap`/
`jitterMs`/`backfillPeriod` for each — never `run`, which isn't serializable
and shouldn't reach the browser anyway. `POST <mount>/api/crons/:name/run`
calls that cron's `run()` immediately and returns `{ ok: true, result }` with
`result` capped the same way as a step result; an unknown name 404s and a
thrown error surfaces as a 500 with its message.

Triggering this way calls `run()` directly — it does **not** go through
the engine's scheduler, so it skips the advisory lock behind
`overlap: "skip"`. A manual run can therefore execute alongside a scheduled
fire of the same cron even when overlap is set to skip.

## Payload capping

Run inputs/outputs, step results, and signal payloads are `jsonb` and can be
arbitrarily large. The API never streams more than `jsonCap` characters of
any one value (default 20 000) — larger values arrive as a preview flagged
`truncated`, with the full serialized size:

```ts
import { createFlowsDashboard } from "iterativeflow/dashboard";

const dashboard = createFlowsDashboard({ engine, jsonCap: 5_000 });
```

The runs _list_ omits `input`/`output` entirely (and trims errors to
`code` + `message`); full payloads appear only in the detail view.

## Theming

The UI is styled on the shadcn/ui token contract: `--background`/`--foreground`,
`--card`, `--primary`, `--muted`, `--accent`, `--destructive`, `--border`,
`--input`, `--ring`, plus a `--radius` scale. It follows the OS light/dark
preference, with a header toggle to override and persist your choice, via a
`.dark` class. Run and attempt statuses have no
shadcn equivalent, so they use an extension palette: `--status-pending`,
`--status-running`, `--status-sleeping`, `--status-awaiting_signal`,
`--status-retrying`, `--status-done`, `--status-failed`, `--status-canceled`,
plus `--status-ok` and `--status-warn`.

Pass `theme` to make it match your app. Token names are typed (no `--`
prefix); values are HSL channels (the shadcn/Franken convention, no color
function). The generated CSS is injected into `<head>` after the built-in
stylesheet, so it wins.

```ts
import { createFlowsDashboard } from "iterativeflow/dashboard";

const dashboard = createFlowsDashboard({
  engine,
  theme: {
    light: { primary: "240 60% 45%", radius: "0.5rem" },
    dark: { primary: "240 55% 65%" },
  },
});
```

`light` maps onto `:root`, `dark` onto `.dark`. For anything the token maps
can't express, a `css` string is appended verbatim — trusted host config, so
don't build it from untrusted input.

## JSON API

The UI is a thin client over these endpoints; script against them freely.
All timestamps are ISO-8601 strings.

| Route                      | Query / body                                                                                              | Returns                                                  |
| -------------------------- | --------------------------------------------------------------------------------------------------------- | -------------------------------------------------------- |
| `GET  api/health`          | —                                                                                                         | `HealthReport` (per-process)                             |
| `GET  api/runs`            | `name`, `status` (comma-separated), `tag`, `since`, `until`, `limit` (≤500), `cursorCreatedAt`+`cursorId` | `{ runs: [...], next: { createdAt, id } \| null }`       |
| `GET  api/runs/:id`        | —                                                                                                         | run + steps + timers + signals, JSON values capped       |
| `POST api/runs/:id/cancel` | `{ "reason"?: string }`                                                                                   | `{ ok: true }`                                           |
| `POST api/runs/:id/retry`  | `{}`                                                                                                      | `RetryResult`; 409 when the run isn't `failed`           |
| `POST api/runs/:id/signal` | `{ "name": string, "payload"?: unknown }`                                                                 | `SignalDeliveryResult`; 422 invalid payload, 409 expired |
| `GET  api/crons`           | —                                                                                                         | `{ crons: [...] }` (no `run`)                            |
| `POST api/crons/:name/run` | `{}`                                                                                                      | `{ ok: true, result }`; 404 if unknown, 500 on throw     |

Invalid filters return 400 with `{ error }`; unknown runs 404; wrong methods
405; mutations without a JSON content type 415.
