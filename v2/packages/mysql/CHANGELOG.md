# @iterativeflow/mysql

## 2.0.0-alpha.6

### Minor Changes

- 864f04b: `inTx` for MySQL and SQLite — transactional-enqueue parity with Postgres.

  `@iterativeflow/mysql` and `@iterativeflow/sqlite` now export `inTx(driver, fn, opts?)`, mirroring
  `@iterativeflow/postgres`: it runs `fn` in one transaction with a `Backend` bound to it, so a `submit`
  (startRun + enqueue) commits atomically with the caller's own writes — the flow runs iff the domain row
  commits, and a throw rolls back both. It stays poll-first (a short bounded tx, not a pinned `LISTEN`)
  and closes only the enqueue-drop window (the reconciler still recovers a worker dying mid-flow). Not in
  core `submit` because it needs a caller-joinable transaction — DynamoDB and Redis can't offer it. Also
  documents the pattern in the postgres README.

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

- e391196: New backend: **`@iterativeflow/mysql`** — the four ports over one InnoDB database. State and dispatch
  share the database, so the transactional outbox commits in one `BEGIN…COMMIT` and `claim` uses
  `FOR UPDATE SKIP LOCKED`. Because MySQL has no `RETURNING`, first-writer-wins reads `affectedRows`
  (`INSERT IGNORE`) and `claim` is a `SELECT … FOR UPDATE SKIP LOCKED` + `UPDATE` — same semantics as
  the Postgres backend. Timestamps are BIGINT epoch ms, JSON is LONGTEXT, insertion order is an
  `AUTO_INCREMENT` seq. Wakeup is in-process. Passes all nine conformance suites. Requires MySQL 8+;
  `mysql2` is a peer dependency.

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
