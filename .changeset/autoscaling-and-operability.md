---
"@iterativeflow/core": minor
"@iterativeflow/postgres": patch
"@iterativeflow/mysql": patch
"@iterativeflow/sqlite": patch
"@iterativeflow/dynamodb": patch
"@iterativeflow/mongodb": patch
"@iterativeflow/redis": patch
"@iterativeflow/durable-objects": patch
"@iterativeflow/dashboard": patch
"@iterativeflow/conformance": patch
---

Autoscaling backlog primitive, plus operability for rolling deploys and pooled/serverless databases.

- **Autoscaling backlog.** `engine.pendingWork(names?)` returns claimable jobs + due timers + due
  crons as one number, served over HTTP at the dashboard's `GET /api/metrics`. Postgres also ships a
  `pending_work(flow_names, as_of)` SQL function so KEDA's Postgres scaler can read it directly,
  including scaling to and from zero. Counting due timers/crons (not just queued jobs) is what wakes a
  scaled-to-zero worker for a durable `ctx.sleep` or a cron.
- **`engine.check()`** — a startup probe that throws a clear error if the backend schema is missing or
  unreachable, instead of the worker loop silently retrying query errors.
- **`redeployParked` metric** — fires when a claimed run parks for `unknown_flow`/`flow_drift`, so a
  rolling deploy can alert on runs stuck waiting for a flow version that didn't come back.
- **SQLite safe defaults.** `applySchema` now sets WAL, `busy_timeout`, and `synchronous=NORMAL` for a
  concurrent, durable file store. Opt out via `ApplySchemaOpts.pragmas` (Durable Objects, which manage
  their own durability, skip them automatically).
- **Postgres autovacuum.** The high-churn `job` table is created with aggressive autovacuum so a queue
  workload doesn't bloat; set once on create, so a later operator `ALTER` is never reset.
- **MySQL isolation.** READ COMMITTED is now set per transaction (safe behind a connection pooler).
  `mysqlPool(pool, { setIsolation: false })` skips it for PlanetScale/Vitess, where a server-default
  READ COMMITTED avoids tainting pooled connections.
