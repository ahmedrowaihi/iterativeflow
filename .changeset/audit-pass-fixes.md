---
"@iterativeflow/core": minor
"@iterativeflow/mysql": patch
"@iterativeflow/dynamodb": patch
"@iterativeflow/mongodb": patch
"@iterativeflow/redis": patch
"@iterativeflow/postgres": patch
"@iterativeflow/sqlite": patch
"@iterativeflow/webhooks": patch
---

Pre-release audit pass — correctness, consistency, and hardening fixes:

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
