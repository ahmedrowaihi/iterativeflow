---
"@iterativeflow/core": minor
"@iterativeflow/conformance": minor
"@iterativeflow/postgres": minor
"@iterativeflow/mysql": minor
"@iterativeflow/sqlite": minor
"@iterativeflow/durable-objects": minor
"@iterativeflow/dynamodb": minor
"@iterativeflow/mongodb": minor
"@iterativeflow/redis": minor
"@iterativeflow/memory": minor
"@iterativeflow/dashboard": minor
"@iterativeflow/webhooks": minor
---

**Fixes**

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
