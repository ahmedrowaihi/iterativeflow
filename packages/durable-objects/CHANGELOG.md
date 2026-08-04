# @iterativeflow/durable-objects

## 2.0.1

### Patch Changes

- @iterativeflow/core@2.0.1
- @iterativeflow/sqlite@2.0.1

## 2.0.0

### Minor Changes

- d35db90: New: **`@iterativeflow/durable-objects`** — run iterativeflow inside a Cloudflare Durable Object on
  its built-in SQLite storage. It's the `@iterativeflow/sqlite` backend driven through a thin `Sql`
  adapter over `ctx.storage.sql` (`createDurableObjectBackend(storage)` + `applySchema`), so one DO
  becomes a self-contained, strongly-consistent durable-execution engine at the edge with no external
  database. No dependency beyond core + sqlite (the `SqlStorage` type is structural). Passes the same
  nine conformance suites as every other backend, verified against Node's synchronous `node:sqlite`,
  which matches the DO storage shape. A DO serves one request at a time (single-writer by
  construction); the outbox relies on the DO's invocation-level atomicity rather than a manual
  transaction, which DO SQLite forbids.

### Patch Changes

- d483f4f: Autoscaling backlog primitive, plus operability for rolling deploys and pooled/serverless databases.

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

- Updated dependencies [d35db90]
- Updated dependencies [d483f4f]
- Updated dependencies [d35db90]
- Updated dependencies [d35db90]
- Updated dependencies [d35db90]
- Updated dependencies [d35db90]
- Updated dependencies [d35db90]
- Updated dependencies [d35db90]
- Updated dependencies [d35db90]
- Updated dependencies [d35db90]
- Updated dependencies [d35db90]
- Updated dependencies [d35db90]
- Updated dependencies [d35db90]
- Updated dependencies [d35db90]
- Updated dependencies [d35db90]
- Updated dependencies [d35db90]
- Updated dependencies [d35db90]
- Updated dependencies [d35db90]
- Updated dependencies [d35db90]
- Updated dependencies [d35db90]
- Updated dependencies [d35db90]
  - @iterativeflow/core@2.0.0
  - @iterativeflow/sqlite@2.0.0

## 2.0.0-alpha.11

### Patch Changes

- Updated dependencies [0101884]
  - @iterativeflow/core@2.0.0-alpha.11
  - @iterativeflow/sqlite@2.0.0-alpha.11

## 2.0.0-alpha.10

### Patch Changes

- Updated dependencies [f84d352]
  - @iterativeflow/core@2.0.0-alpha.10
  - @iterativeflow/sqlite@2.0.0-alpha.10

## 2.0.0-alpha.9

### Patch Changes

- Updated dependencies [b8a9bb2]
  - @iterativeflow/core@2.0.0-alpha.9
  - @iterativeflow/sqlite@2.0.0-alpha.9

## 2.0.0-alpha.8

### Patch Changes

- Updated dependencies [33d361c]
  - @iterativeflow/core@2.0.0-alpha.8
  - @iterativeflow/sqlite@2.0.0-alpha.8

## 2.0.0-alpha.7

### Patch Changes

- @iterativeflow/core@2.0.0-alpha.7
- @iterativeflow/sqlite@2.0.0-alpha.7

## 2.0.0-alpha.6

### Patch Changes

- Updated dependencies [864f04b]
  - @iterativeflow/sqlite@2.0.0-alpha.6
  - @iterativeflow/core@2.0.0-alpha.6

## 2.0.0-alpha.5

### Patch Changes

- Updated dependencies [f5df1e8]
  - @iterativeflow/core@2.0.0-alpha.5
  - @iterativeflow/sqlite@2.0.0-alpha.5

## 2.0.0-alpha.4

### Patch Changes

- Updated dependencies [3a1d828]
  - @iterativeflow/core@2.0.0-alpha.4
  - @iterativeflow/sqlite@2.0.0-alpha.4

## 2.0.0-alpha.3

### Patch Changes

- Updated dependencies [5b07ed6]
- Updated dependencies [acbe2bb]
- Updated dependencies [2257a3e]
- Updated dependencies [539a1c2]
- Updated dependencies [12f3baa]
  - @iterativeflow/core@2.0.0-alpha.3
  - @iterativeflow/sqlite@2.0.0-alpha.3

## 2.0.0-alpha.2

### Minor Changes

- f191234: New: **`@iterativeflow/durable-objects`** — run iterativeflow inside a Cloudflare Durable Object on
  its built-in SQLite storage. It's the `@iterativeflow/sqlite` backend driven through a thin `Sql`
  adapter over `ctx.storage.sql` (`createDurableObjectBackend(storage)` + `applySchema`), so one DO
  becomes a self-contained, strongly-consistent durable-execution engine at the edge with no external
  database. No dependency beyond core + sqlite (the `SqlStorage` type is structural). Passes the same
  nine conformance suites as every other backend, verified against Node's synchronous `node:sqlite`,
  which matches the DO storage shape. A DO serves one request at a time (single-writer by
  construction); the outbox relies on the DO's invocation-level atomicity rather than a manual
  transaction, which DO SQLite forbids.

### Patch Changes

- Updated dependencies [e1ef077]
- Updated dependencies [3377316]
- Updated dependencies [a624058]
- Updated dependencies [f7bf20f]
- Updated dependencies [dc2b059]
- Updated dependencies [7a32846]
- Updated dependencies [11d3aa2]
  - @iterativeflow/core@2.0.0-alpha.2
  - @iterativeflow/sqlite@2.0.0-alpha.2
