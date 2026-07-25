---
"@iterativeflow/core": minor
"@iterativeflow/dynamodb": minor
"@iterativeflow/postgres": minor
---

Consumer migration, schema-ownership, type-safety, replay-safety, and correctness work:

- **Fix: the attempt cap no longer kills long-lived/looping runs.** `markRunning` bumps `attempts`
  on every claim, and each durable resume (a `ctx.sleep` wake, a signal, a sequential `ctx.invoke`)
  is a fresh claim — so any run dispatched more than `maxAttempts` (default 10) times was failed with
  `RUN_ATTEMPTS_EXHAUSTED` despite zero failures, contradicting the durable-sleep guarantee. Attempts
  now reset on forward-progress suspends (`sleeping`/`awaiting_signal`/`awaiting_child`) — the
  `suspendRun` write zeroes the dispatch counter in the same write for those statuses; the
  poison-pill cap still fires on no-progress re-claims.
- **Robustness/perf**: the resident `engine.run()` loop routes background-tick rejections to a
  `metrics.tickError` hook instead of letting an unhandled rejection crash the process; `loadRun`
  (Postgres) and the `drainTimers`/`reconcile` re-enqueue loops now run their independent I/O in
  parallel; a new `store.loadRunRow` lets `invoke`/`result` read just the run row instead of the full
  snapshot; DynamoDB `orphanedRuns` derives its reconcilable set from `RECONCILABLE_STATUSES` (no
  per-backend drift).
- **Tests**: a shared `engineConformance` suite now runs the composed engine behaviors
  (retry/dead-letter, signal resume, cancel cascade to grandchild depth) against all three backends,
  not just memory.

- **Flow drift guard**: each step memo records the `kind:label` of the `ctx` call that made it; on
  replay the executor compares it to the call now issued at that cursor. A flow body reordered or
  refactored under a live run (without a `version` bump) is detected and, per `driftPolicy` on the
  engine (or overridden per-flow), parks the run recoverably (`flow_drift`, default) or fails it
  (`FLOW_DRIFT`). Restores v1's
  static drift detection as a runtime check. Adds a nullable `shape` column/attribute to the step memo
  in all three backends (additive; pre-existing memos skip the check). See `docs/v2/CONTRACTS.md`.

- **Typed flows & signals** (restores v1 per-flow type-safety, adds typed signals): `submit` returns
  a `RunHandle<O, S>` so `result` recovers the flow's output type `O` (was `unknown`), and a flow's
  `signals` map types both `ctx.signal(name)` on the await side and `signal(handle, name, payload)` on
  the send side — a wrong signal name or payload is a compile error on both ends. A `signals` entry is
  any **Standard-Schema** validator (zod/valibot/arktype), just like `input`: the payload is validated
  (and parsed) as the flow consumes it, and a bad one fails the run. `signalType<T>()` is the
  type-only escape hatch. `RunHandle` is a `string`, so plain-string `result`/`signal` and stored run ids keep
  working. See `docs/v2/CONTRACTS.md`.
- **DynamoDB consistency**: strongly-consistent reads on the durable decision path — the `loadRun`
  replay Query, the base-table point reads, and `childrenOf` (which drives the cancel cascade — a
  stale read there let a just-spawned child escape cancellation permanently). GSI reads stay
  eventually-consistent (CAS-guarded); observability scans stay eventual (no wasted RCU).

- **`serverlessTick`** (core, plus `engine.serverlessTick`): one invocation fires due crons,
  reconciles orphans, drains due timers, and advances a batch — a cron-Lambda entrypoint with no
  resident daemon. A durable `ctx.sleep` survives across invocations. Size `leaseMs` ≤ the
  invocation timeout.
- **DynamoDB `tableSpec` + `REQUIRED_IAM_ACTIONS`**: provision the table + GSI in your own IaC;
  the IAM list names `TransactWriteItems`/`ConditionCheckItem`, the two a CDK `grantReadWriteData`
  omits. `claim` now paginates the JOB partition so due jobs are not starved behind a backlog of
  leased/future-dated jobs.
- **Postgres `drizzleSchema()` + `iterativeflow-pg-drizzle` bin**: emit a consumer-owned drizzle
  schema for typed reads, foreign keys to `workflow.run`, and your own drizzle-kit migrations —
  generated (not re-exported) so it targets your installed drizzle. Drift-tested against `ddl()`
  on real Postgres; verified on drizzle stable (`0.45`) and the `1.0` beta.
