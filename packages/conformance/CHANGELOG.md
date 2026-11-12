# @iterativeflow/conformance

## 3.2.0

### Minor Changes

- 68123ec: **Fixes**

  - `nextWakeAt` (and `serverlessTick`'s) now includes the next cron fire, so a cron-only deployment is re-invoked, and reports wake-now while due timers are left over. The test harness fires due crons when it drains, and `settle` still reports a stalled run when crons are registered.
  - A run canceled between claim and start no longer executes its body; one canceled while its body runs no longer reports itself completed or wakes its parent twice.
  - A purge never deletes a finished child while its parent is still live (the parent would wait forever), and a run retried mid-purge is no longer deleted. A retried parent whose child was purged fails its join with `CHILD_GONE` instead of waiting forever.
  - A cron's `input` is validated against the flow's schema (and `maxPayloadBytes`) at `registerCron`.
  - A completed run clears its own pending timer; a throwing metrics callback no longer turns a finished run into a retry.
  - DynamoDB: purge keeps no stale idempotency marker; `loadRun` pages past 1 MB; a re-armed timer isn't fired early; signals keep delivery order across processes; `REQUIRED_IAM_ACTIONS` adds `BatchGetItem`/`BatchWriteItem`; a sharded claim isn't starved by other shards.
  - MongoDB: `cancelRuns`/`retryRuns` guard status and run in a transaction; `dueBatch` fires each timer once; a sharded claim isn't starved; duplicate writes no longer abort a caller's `inTx` transaction.
  - Redis: a claim isn't starved by leased or other-shard jobs; re-spawning an existing child no longer overwrites it; purge removes the job from the queue.
  - MySQL: idempotent inserts no longer swallow non-duplicate errors (e.g. truncation). Postgres/MySQL: a failed ROLLBACK destroys the connection instead of returning it to the pool.
  - op-sqlite: overlapping transactions on the single connection are serialized.
  - Dashboard: API calls work under a `basePath` without a trailing slash.
  - Webhooks: `webhookSignalBridge` takes the engine instead of the backend, so deliveries obey `maxPayloadBytes`. Pass `engine` where you passed `backend`.

  **New**

  - `engine.children(runId)`.
  - `backendConformance(label, makeBackend, { concurrentWriters })` runs every suite — use it instead of listing suites.
  - Postgres `createPgListener({ onError })` — connect/`LISTEN` failures are reported (default `console.error`) instead of retried silently.
  - `engine.listRuns` rejects a non-positive-integer `page.limit`.

  **Backend SPI (`@iterativeflow/core/backend`) — breaking for custom backends**

  - `Store.deleteRunsOlderThan` is removed; implement `deleteRuns` only. New `Store.nextCronAt()`. `Store.markTerminal` returns whether it made the run terminal.
  - `Outbox.enqueue` and `SpawnRequest.enqueue` are removed; `CronSpec.overlap` is required.
  - The purge/run-set helpers are replaced by `listSelection`/`cancelSelection`/`retrySelection`/`purgeSelection` with `runMatcher`/`runSelectionSql`; `orphanedRunsSql` renders its own status tuples. New: `shardAdmits`, `enqueueFields`, `resetsAttempts`, `FAN_OUT_CHUNK`.
  - `runTick` requires `leaseMs`.

### Patch Changes

- Updated dependencies [68123ec]
  - @iterativeflow/core@3.2.0

## 3.1.0

### Patch Changes

- Updated dependencies [528b8ee]
  - @iterativeflow/core@3.1.0

## 3.0.0

### Patch Changes

- Updated dependencies [b4707aa]
- Updated dependencies [8a9d860]
- Updated dependencies [aed7cc2]
- Updated dependencies [a905beb]
- Updated dependencies [2d86163]
- Updated dependencies [57a5c90]
  - @iterativeflow/core@3.0.0

## 2.7.0

### Patch Changes

- @iterativeflow/core@2.7.0

## 2.6.0

### Patch Changes

- Updated dependencies [5a87a80]
  - @iterativeflow/core@2.6.0

## 2.5.0

### Patch Changes

- Updated dependencies [b947bc7]
  - @iterativeflow/core@2.5.0

## 2.4.0

### Patch Changes

- Updated dependencies [2f1bb99]
- Updated dependencies [c5443d5]
- Updated dependencies [6ed85c5]
- Updated dependencies [2000935]
- Updated dependencies [789e7fc]
- Updated dependencies [6f2f7b4]
- Updated dependencies [9eb11e2]
- Updated dependencies [cad0b53]
- Updated dependencies [5c2c9d5]
- Updated dependencies [a6b79cc]
- Updated dependencies [8a02c61]
- Updated dependencies [02147f8]
- Updated dependencies [2367975]
  - @iterativeflow/core@2.4.0

## 2.3.0

### Patch Changes

- Updated dependencies [d0dc42f]
- Updated dependencies [ff33a4b]
  - @iterativeflow/core@2.3.0

## 2.2.0

### Patch Changes

- Updated dependencies [f765a77]
- Updated dependencies [ce5765c]
  - @iterativeflow/core@2.2.0

## 2.1.1

### Patch Changes

- @iterativeflow/core@2.1.1

## 2.1.0

### Patch Changes

- @iterativeflow/core@2.1.0

## 2.0.1

### Patch Changes

- @iterativeflow/core@2.0.1

## 2.0.0

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

- d35db90: Declare `license: MIT` and the repository field in every package manifest — the alpha.1 tarballs showed as "Proprietary" on npm.
- d35db90: First public alpha of iterativeflow v2 — a ground-up durable-execution engine.

  - **Four-port architecture** (store / queue / timer / wakeup) with a transactional-outbox seam: every durable write commits its side-effects (child spawns, enqueues, timers, signal consumption) atomically. One durable write per step.
  - **Three backends against one conformance suite**: in-memory (reference), Postgres (`BEGIN…COMMIT`, `SKIP LOCKED`, proven under real concurrency), DynamoDB (single-table, `TransactWriteItems`, two-phase fan-out past the 100-item cap).
  - **Authoring**: imperative `defineFlow` + a fully-typed accumulator `builder`, per-step policy (retries, timeout, transient/permanent classification, AbortSignal), Standard-Schema input validation.
  - **Durable primitives**: steps with exactly-once memos, `sleep`/`sleepUntil`, child workflows via `ctx.invoke`, external signals via a durable inbox, idempotent submits, atomic batch dispatch, and Postgres transactional enqueue (`inTx`).
  - **Reliability**: run-level retry with backoff, dead-letter attempt cap, orphan reconciler, wake-survives-ack queue versioning, cancel with cascade, retry-a-failed-run preserving memos.
  - **Operations**: `createEngine` facade with a resident worker loop, cron (CAS single-fire, overlap-skip), `listRuns`/`status`/`health` query surface, gated durable event log + metrics hooks, and a mountable dashboard (`fetch` handler + self-contained UI).
  - **Split entries**: `@iterativeflow/core` for app authors, `@iterativeflow/core/backend` for backend implementors.

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
  - @iterativeflow/core@2.0.0

## 2.0.0-alpha.11

### Patch Changes

- Updated dependencies [0101884]
  - @iterativeflow/core@2.0.0-alpha.11

## 2.0.0-alpha.10

### Patch Changes

- Updated dependencies [f84d352]
  - @iterativeflow/core@2.0.0-alpha.10

## 2.0.0-alpha.9

### Patch Changes

- Updated dependencies [b8a9bb2]
  - @iterativeflow/core@2.0.0-alpha.9

## 2.0.0-alpha.8

### Patch Changes

- Updated dependencies [33d361c]
  - @iterativeflow/core@2.0.0-alpha.8

## 2.0.0-alpha.7

### Patch Changes

- @iterativeflow/core@2.0.0-alpha.7

## 2.0.0-alpha.6

### Patch Changes

- @iterativeflow/core@2.0.0-alpha.6

## 2.0.0-alpha.5

### Patch Changes

- Updated dependencies [f5df1e8]
  - @iterativeflow/core@2.0.0-alpha.5

## 2.0.0-alpha.4

### Patch Changes

- Updated dependencies [3a1d828]
  - @iterativeflow/core@2.0.0-alpha.4

## 2.0.0-alpha.3

### Patch Changes

- Updated dependencies [5b07ed6]
- Updated dependencies [acbe2bb]
- Updated dependencies [2257a3e]
- Updated dependencies [539a1c2]
- Updated dependencies [12f3baa]
  - @iterativeflow/core@2.0.0-alpha.3

## 2.0.0-alpha.2

### Patch Changes

- a624058: Declare `license: MIT` and the repository field in every package manifest — the alpha.1 tarballs showed as "Proprietary" on npm.
- Updated dependencies [e1ef077]
- Updated dependencies [3377316]
- Updated dependencies [a624058]
- Updated dependencies [f7bf20f]
- Updated dependencies [dc2b059]
- Updated dependencies [11d3aa2]
  - @iterativeflow/core@2.0.0-alpha.2

## 2.0.0-alpha.1

### Patch Changes

- First public alpha of iterativeflow v2 — a ground-up durable-execution engine.

  - **Four-port architecture** (store / queue / timer / wakeup) with a transactional-outbox seam: every durable write commits its side-effects (child spawns, enqueues, timers, signal consumption) atomically. One durable write per step.
  - **Three backends against one conformance suite**: in-memory (reference), Postgres (`BEGIN…COMMIT`, `SKIP LOCKED`, proven under real concurrency), DynamoDB (single-table, `TransactWriteItems`, two-phase fan-out past the 100-item cap).
  - **Authoring**: imperative `defineFlow` + a fully-typed accumulator `builder`, per-step policy (retries, timeout, transient/permanent classification, AbortSignal), Standard-Schema input validation.
  - **Durable primitives**: steps with exactly-once memos, `sleep`/`sleepUntil`, child workflows via `ctx.invoke`, external signals via a durable inbox, idempotent submits, atomic batch dispatch, and Postgres transactional enqueue (`inTx`).
  - **Reliability**: run-level retry with backoff, dead-letter attempt cap, orphan reconciler, wake-survives-ack queue versioning, cancel with cascade, retry-a-failed-run preserving memos.
  - **Operations**: `createEngine` facade with a resident worker loop, cron (CAS single-fire, overlap-skip), `listRuns`/`status`/`health` query surface, gated durable event log + metrics hooks, and a mountable dashboard (`fetch` handler + self-contained UI).
  - **Split entries**: `@iterativeflow/core` for app authors, `@iterativeflow/core/backend` for backend implementors.

- Updated dependencies
  - @iterativeflow/core@2.0.0-alpha.1
