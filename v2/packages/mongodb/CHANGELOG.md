# @iterativeflow/mongodb

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
