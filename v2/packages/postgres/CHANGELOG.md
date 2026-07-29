# @iterativeflow/postgres

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

### Minor Changes

- 3a1d828: FlowError.cause capture + a Postgres classify preset (production field report — vod-media-convert).

  - **`FlowError.cause`** — `toFlowError` now walks a thrown error's `.cause` chain (bounded depth) and
    flattens it into the persisted error, so a wrapper like `DrizzleQueryError` (generic "Failed query:
    rollback", the real pg error on `.cause`) no longer reduces a run record to `[object Object]`. This
    removes the need for a `failNormalized`/`dbStep` workaround.
  - **`pgClassify`** (`@iterativeflow/postgres`) — a ready `StepPolicy.classify` preset: constraint, data,
    and syntax/access errors are permanent (fail fast), while connection drops, statement timeouts,
    deadlocks, and serialization failures stay transient (retry). Walks the `.cause` chain for the SQLSTATE.
  - **Docs** — an error-sink recipe (`observe.sink` capturing `FlowError.cause`), idempotent-step guidance,
    and a note that `maxAttempts` already bounds a stalled-step reclaim loop (no blind re-dispatch).

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

- f7bf20f: Consumer migration, schema-ownership, type-safety, replay-safety, and correctness work:

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

- dc2b059: Close out the deferred parity items:

  - **Invoke depth cap**: a per-run `depth` (0 for a submit, parent+1 per child) and `policy.maxDepth`
    (default 32) reject runaway `ctx.invoke` recursion before spawning. Persisted on all three backends.
  - **Retention**: `Store.deleteRunsOlderThan(before, limit)` + `engine.prune(olderThanMs, limit?)`
    delete terminal runs (and their steps/signals/events) past a cutoff; not wired into the loop —
    schedule it yourself. Runs now carry `createdAt` (on `RunRow`), stamped once from the engine clock
    at submit/spawn so it agrees with the prune cutoff under any injected clock.
  - **`ctx.log(message, data?)`**: a durable, replay-suppressed run log line to the event sink.
  - **`defineContract`**: a type-only I/O + signal contract so a caller that doesn't own a flow's body
    (another service, the Go worker) can `submit`/`result`/`signal` it with full type-safety.
  - **Health liveness**: `Queue.depth(now)` (backlog / in-flight / oldest-claimable age) and
    `engine.liveness()` for a k8s readiness probe.
  - **Tracing**: a `Tracer` hook on `ObserveOpts` emitting one durable span per executed step —
    `traceId` stable per run, `spanId` derived from the step cursor (idempotent across replay),
    dependency-free. Wire it to `@opentelemetry/api`.
  - **Live progress push** (opt-in, Postgres): `applyProgressTrigger` + `createPgListener.watch(runId)`
    / `onProgress(cb)` — a third `LISTEN/NOTIFY` channel on the existing socket, off the worker hot path.

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

- a624058: Declare `license: MIT` and the repository field in every package manifest — the alpha.1 tarballs showed as "Proprietary" on npm.
- 11d3aa2: Audit sweep — correctness, type-safety, and naming consistency:

  - **Typed fan-out inputs**: `ctx.invoke([{ flow, input }, …])` now type-checks each child `input`
    against ITS own flow (was `any` on the many-form), inferred from a flow tuple so the joined
    outputs stay per-child typed. Replaces the spec-tuple-keyed `InvokeOutputs` with `FlowOutputs` +
    `InvokeSpecFor` on the public surface.
  - **DynamoDB `startManyRuns` batches atomic chunks**: the earlier per-run create (one write per run,
    unbounded fan-out on a large `submitMany`) is replaced by within-batch idempotency-key dedup +
    atomic `TransactWriteItems` chunks bounded by the 100-item cap, falling back to per-run create only
    for a chunk a concurrent creator races. Restores per-chunk all-or-none without regressing dedup.
  - **Renames (breaking)**: the type-only signal helper `type<T>()` → `signalType<T>()`; the batch-submit
    spec `SubmitItem` → `SubmitSpec`; the row-limit SPI param `max` → `limit` (`claim`, `dueBatch`,
    `orphanedRuns`, `dueCrons`, `reconcile`, `drainTimers`).
  - **Correctness**: the reconcile lost-parent-wake fires only on a _resolved_ fan-out join (fast-fail
    preserved) instead of any terminal child; cron no longer throws on a valid sparse schedule spanning
    a leap cycle.
  - **Cleanup**: removed the unreachable `failed_terminal` step status and the unused `Queue.release`;
    extracted the triplicated orphan predicate to one shared `isOrphaned`.

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
