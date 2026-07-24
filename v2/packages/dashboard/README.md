# @iterativeflow/dashboard

A read + control dashboard for [iterativeflow](https://github.com/ahmedrowaihi/iterativeflow)
v2. `createDashboard(engine)` returns a single `(Request) => Promise<Response>`
fetch handler — an HTML UI at the base path and a JSON API over the engine's
query and control surface (list runs, inspect a run, cancel, retry, signal).

```bash
npm install @iterativeflow/dashboard @iterativeflow/core
```

```ts
import { createDashboard } from "@iterativeflow/dashboard";

const handler = createDashboard(engine, { basePath: "/admin/flows" });
// Mount on any fetch-native server (Hono, Bun, Next route handler, Workers):
app.all("/admin/flows/*", (c) => handler(c.req.raw));
```

Pass `events` (e.g. the Postgres `listEvents`) to show a run's durable timeline;
omit it if the backend records no events.

> **Unauthenticated by design.** The handler exposes mutations (cancel, retry,
> signal) with no built-in auth — mount it **behind your host's authentication**.
> It is an internal operator surface, not a public endpoint.
