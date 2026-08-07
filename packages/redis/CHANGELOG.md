# @iterativeflow/redis

## 2.1.1

### Patch Changes

- 9028e14: Fix: a flow-name-filtered `claim` no longer skips a job whose run row is gone.

  Previously, such an orphan (a run deleted out of band while its job lingered) resolved to no flow name, failed the `names` filter, and was dropped every tick — never leased, never acked — so it stayed permanently claimable and inflated `queue.depth()` / `oldestClaimableAgeMs` (a `run_at=0` orphan reads as a ~56-year-old backlog item), making a healthy engine's readiness check look stalled. The unfiltered claim path already self-healed (runTick loads the gone run and acks it); the name-filtered path dropped the job before it could be leased.

  The name-filtered claim now leases a run-less job too, so `runTick`'s existing gone-path acks it. (`prune` already deletes a run's job and timer, so the primary orphan source was already handled — this closes the out-of-band case and the claim-path asymmetry.)

  - @iterativeflow/core@2.1.1

## 2.1.0

### Patch Changes

- @iterativeflow/core@2.1.0

## 2.0.1

### Patch Changes

- @iterativeflow/core@2.0.1

## 2.0.0

### Minor Changes

- d35db90: New backend: **`@iterativeflow/redis`** — the four ports over one ioredis connection, for Redis /
  Valkey / Dragonfly (single node). State and dispatch share the keyspace, so the transactional outbox
  commits as one Lua script; the Queue is sorted-set lease-CAS (crash recovery via lease expiry), the
  Timer a `fireAt` sorted set, and Wakeup is in-process. Durability is the server's AOF config — the
  fast, loss-tolerant tier. Passes the same store/queue/timer/wakeup/outbox/signal/reconcile/cron/engine
  conformance suites as the Postgres and DynamoDB backends. `ioredis` is a peer dependency.

### Patch Changes

- d35db90: Pre-release audit pass — correctness, consistency, and hardening fixes:

  - **core (cron at-most-once bug):** `runDueCrons` now starts the occurrence's idempotent run BEFORE
    advancing the schedule CAS. Previously a crash between `advanceCron` and `startRun` dropped the
    occurrence silently — at-most-once delivery inside an otherwise at-least-once engine. Also adds the
    `orphanedRunsSql` and `assertSqlIdentifier` backend-SPI helpers.
  - **mysql (atomicity bug):** transactions now run at READ COMMITTED, not MySQL's REPEATABLE READ
    default, so a concurrent first-writer-wins checkpoint's re-read sees the winner's just-committed
    row — matching Postgres, the model the store targets. Surfaced by a new concurrency conformance
    test.
  - **dynamodb / mongodb (lease version):** `claim` captures the job `version` from the atomic lease
    write (`ReturnValues: ALL_NEW` / the `findOneAndUpdate` result) rather than a stale pre-lease read,
    matching the other six backends.
  - **redis (performance):** `listRuns` scans the run index in bounded windows instead of loading every
    run on an interactive page.
  - **postgres:** the job `version` seeds at 1 like every other backend; the orphan query uses the
    shared `orphanedRunsSql`. **sqlite / mysql** share the same builder (one predicate, one home).
  - **hardening:** the webhook `hmacVerifier` refuses an empty secret at construction (fail closed);
    the SQL backends validate the schema/table-prefix identifier at construction.

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

### Minor Changes

- eab5c26: New backend: **`@iterativeflow/redis`** — the four ports over one ioredis connection, for Redis /
  Valkey / Dragonfly (single node). State and dispatch share the keyspace, so the transactional outbox
  commits as one Lua script; the Queue is sorted-set lease-CAS (crash recovery via lease expiry), the
  Timer a `fireAt` sorted set, and Wakeup is in-process. Durability is the server's AOF config — the
  fast, loss-tolerant tier. Passes the same store/queue/timer/wakeup/outbox/signal/reconcile/cron/engine
  conformance suites as the Postgres and DynamoDB backends. `ioredis` is a peer dependency.

### Patch Changes

- e1ef077: Pre-release audit pass — correctness, consistency, and hardening fixes:

  - **core (cron at-most-once bug):** `runDueCrons` now starts the occurrence's idempotent run BEFORE
    advancing the schedule CAS. Previously a crash between `advanceCron` and `startRun` dropped the
    occurrence silently — at-most-once delivery inside an otherwise at-least-once engine. Also adds the
    `orphanedRunsSql` and `assertSqlIdentifier` backend-SPI helpers.
  - **mysql (atomicity bug):** transactions now run at READ COMMITTED, not MySQL's REPEATABLE READ
    default, so a concurrent first-writer-wins checkpoint's re-read sees the winner's just-committed
    row — matching Postgres, the model the store targets. Surfaced by a new concurrency conformance
    test.
  - **dynamodb / mongodb (lease version):** `claim` captures the job `version` from the atomic lease
    write (`ReturnValues: ALL_NEW` / the `findOneAndUpdate` result) rather than a stale pre-lease read,
    matching the other six backends.
  - **redis (performance):** `listRuns` scans the run index in bounded windows instead of loading every
    run on an interactive page.
  - **postgres:** the job `version` seeds at 1 like every other backend; the orphan query uses the
    shared `orphanedRunsSql`. **sqlite / mysql** share the same builder (one predicate, one home).
  - **hardening:** the webhook `hmacVerifier` refuses an empty secret at construction (fail closed);
    the SQL backends validate the schema/table-prefix identifier at construction.

- Updated dependencies [e1ef077]
- Updated dependencies [3377316]
- Updated dependencies [a624058]
- Updated dependencies [f7bf20f]
- Updated dependencies [dc2b059]
- Updated dependencies [11d3aa2]
  - @iterativeflow/core@2.0.0-alpha.2
