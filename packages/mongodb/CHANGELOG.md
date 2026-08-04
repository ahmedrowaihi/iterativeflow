# @iterativeflow/mongodb

## 2.1.0

### Minor Changes

- fb858a4: KEDA-native autoscaling for MySQL and MongoDB, matching Postgres.

  - **MySQL:** `applySchema` now creates a `pending_work(flow_names, as_of)` SQL function (claimable jobs + due timers + due crons), so KEDA's mysql scaler can read the backlog directly. `flow_names` is a JSON array (`NULL` = whole backlog); `as_of` is epoch ms. Creating it needs the `CREATE ROUTINE` privilege.
  - **MongoDB:** a new `pendingWorkPipeline(now)` export returns a `$unionWith` aggregation that counts the backlog across the jobs/timers/crons collections in one query — MongoDB has no stored functions and KEDA's mongodb scaler is single-collection, so this is the mongo-side metric.

### Patch Changes

- @iterativeflow/core@2.1.0

## 2.0.1

### Patch Changes

- @iterativeflow/core@2.0.1

## 2.0.0

### Minor Changes

- d35db90: `inTx` for MongoDB — completing transactional-enqueue across all tx-capable backends.

  `@iterativeflow/mongodb` now exports `inTx(client, fn, opts?)`: it runs `fn` in one transaction with a
  `Backend` bound to a caller session, so a `submit` (startRun + enqueue) commits atomically with the
  caller's own writes — the flow runs iff the domain row commits, and a throw rolls back both. The bound
  session threads through the submit write path; the store's internal transaction helper runs inline in
  the caller's session (MongoDB can't nest transactions). Requires a replica set. Only the write path
  joins the transaction — reads via the bound backend do not observe its uncommitted writes (unlike a SQL
  read-your-own-write). DynamoDB and Redis still can't offer this (no caller-joinable transaction).

- d35db90: New backend: **`@iterativeflow/mongodb`** — the four ports over one MongoClient, runs as documents
  (one collection per concern, `_id = runId`). The transactional outbox commits across collections in a
  multi-document transaction, so a replica set is required (as MongoDB mandates for transactions);
  single-document ops like `claim` and `arriveAtJoin` use per-document atomicity. Timestamps are epoch
  ms, insertion order is a per-doc ObjectId, and the idempotency/signal-dedup indexes are partial
  (`$exists`) so unkeyed docs never collide. Wakeup is in-process. Passes all nine conformance suites.
  `mongodb` is a peer dependency.

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

### Minor Changes

- 8c1075a: `inTx` for MongoDB — completing transactional-enqueue across all tx-capable backends.

  `@iterativeflow/mongodb` now exports `inTx(client, fn, opts?)`: it runs `fn` in one transaction with a
  `Backend` bound to a caller session, so a `submit` (startRun + enqueue) commits atomically with the
  caller's own writes — the flow runs iff the domain row commits, and a throw rolls back both. The bound
  session threads through the submit write path; the store's internal transaction helper runs inline in
  the caller's session (MongoDB can't nest transactions). Requires a replica set. Only the write path
  joins the transaction — reads via the bound backend do not observe its uncommitted writes (unlike a SQL
  read-your-own-write). DynamoDB and Redis still can't offer this (no caller-joinable transaction).

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

- b1c88d8: New backend: **`@iterativeflow/mongodb`** — the four ports over one MongoClient, runs as documents
  (one collection per concern, `_id = runId`). The transactional outbox commits across collections in a
  multi-document transaction, so a replica set is required (as MongoDB mandates for transactions);
  single-document ops like `claim` and `arriveAtJoin` use per-document atomicity. Timestamps are epoch
  ms, insertion order is a per-doc ObjectId, and the idempotency/signal-dedup indexes are partial
  (`$exists`) so unkeyed docs never collide. Wakeup is in-process. Passes all nine conformance suites.
  `mongodb` is a peer dependency.

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
