# @iterativeflow/core

## 2.0.0-alpha.11

### Patch Changes

- 0101884: `@iterativeflow/sqlite`: the op-sqlite adapter now retries `BEGIN IMMEDIATE` on `SQLITE_BUSY`.

  Under concurrent writers, `BEGIN IMMEDIATE` can return `SQLITE_BUSY` when another writer holds the
  write lock. `opSqliteDb` now retries the acquisition with async exponential backoff (~10→160ms, 5
  attempts) before surfacing the error — rather than a `PRAGMA busy_timeout`, whose native blocking
  would freeze the JS thread on op-sqlite's sync (JSI) path. Only `BEGIN` is retried: once it holds the
  lock the statements inside the transaction don't contend. A non-busy error is never retried.

## 2.0.0-alpha.10

### Minor Changes

- f84d352: Browser- and React-Native-ready core, plus an op-sqlite driver adapter for the SQLite backend.

  **Isomorphic core** — the three `node:`-only couplings are gone, so `@iterativeflow/core` (and every
  isomorphic backend) bundles for the browser and React Native with no Node polyfills:

  - `newId` uses the Web Crypto global (`globalThis.crypto.randomUUID()`) instead of `node:crypto`, and
    throws an actionable error (pass a custom `IdGen`) when a runtime lacks the global.
  - Trace/span id hashing uses a bundled synchronous SHA-256 instead of `node:crypto` — output is
    byte-identical (known-answer + `node:crypto` parity tests), and it only runs when a tracer is wired.
  - The resident-loop sleep is a Web-standard `setTimeout` + `AbortSignal` instead of
    `node:timers/promises` (same timer semantics; the abort listener is removed on resolve, no per-tick
    leak).

  Verified with an `esbuild --platform=browser` bundle of both core entrypoints. All three swaps behave
  identically at runtime; the only new requirement is a Web Crypto global (Node 20+ / browsers /
  polyfilled RN), escape-hatched by the injectable `IdGen`.

  **`@iterativeflow/sqlite`: op-sqlite adapter** — `opSqliteDb` / `createOpSqliteBackend` run the SQLite
  backend on [op-sqlite](https://op-engineering.github.io/op-sqlite) — one code path across React Native
  (native JSI) and the browser (wasm + OPFS) — reusing the whole backend and passing every conformance
  suite. Declared structurally (no dependency on op-sqlite; it's an optional peer beside `@libsql/client`).
  The adapter owns `BEGIN IMMEDIATE`/`COMMIT`/`ROLLBACK` for deterministic commit-on-resolve /
  rollback-on-throw.

## 2.0.0-alpha.9

### Minor Changes

- b8a9bb2: Flow-aware claiming — sharded workers only lease runs they can execute.

  `Queue.claim` (`ClaimOpts`) takes an optional `names?: readonly string[]`: the claim is restricted to
  runs whose flow `name` is in the set. `tickOnce` derives it automatically from the worker's registered
  flows, so `engine.run` / `serverlessTick` shard with zero config — a pod that registers a disjoint
  subset of flows never blind-claims a run it has no handler for.

  **Why:** with partitioned pods (an API pod dispatches many flows; each worker pod registers only a
  few), a blind claim leases a run for an unregistered flow, which parks `unknown_flow` BEFORE
  `markRunning` bumps `attempts` — so it never exhausts to a dead-letter and re-parks on `baseDelayMs`
  forever. Roughly one wrong-pod escape per claim cycle, no error logs; high-cadence flows never
  converge. Filtering the claim by registered name removes the bounce at the source.

  - `names` omitted ⇒ no filter (a monolith claims everything — unchanged behavior).
  - Matches on `name` only: a registered name at an unregistered _version_ still leases and then parks
    for redeploy — the intended rolling-deploy handoff, not a shard miss.
  - Every backend implements the filter (SQL `LEFT JOIN run`; memory/redis/mongodb/dynamodb look up the
    run's name), proven by a new `claimFilterConformance` case across all eight backends.

## 2.0.0-alpha.8

### Minor Changes

- 33d361c: `pollTimeoutMs` — bound the resident loop's DB poll so a dead Postgres connection can't silently freeze it.

  The resident worker loop does one `await` per cycle on the DB poll (drain due timers + claim a batch). A
  dropped/black-holed connection — RDS failover, PgBouncer killing a pinned socket — leaves that query
  awaiting a dead socket forever: the process stays alive but stops doing work, with no error. `tickOnce` /
  `engine.run` now bound the poll with `pollTimeoutMs` (default 30s via `createEngine`; `0` disables — an
  in-memory backend never hangs). On timeout the poll rejects `PollTimeoutError`; the resident loop already
  catches tick errors, so it logs via `observe.metrics.tickError` and re-polls on a fresh pooled connection.
  Bounds the poll only, never step execution.

## 2.0.0-alpha.7

## 2.0.0-alpha.6

## 2.0.0-alpha.5

### Minor Changes

- f5df1e8: `ctx.signal(name, { timeoutMs })` — await a signal with a deadline.

  Resolves `{ received: true, payload }` if the signal arrives within `timeoutMs`, else `{ received: false }`.
  A plain `ctx.signal` wait parks until the signal arrives; the timed form can now give up. The timeout
  decision is **linearizable with the durable inbox**: `postSignal` bumps the run's dispatch version as it
  delivers, and the timeout commits under a `requireVersion` guard that write-conflicts on that same job row on
  every backend (SQL `FOR UPDATE`, redis Lua, mongo doc-conflict, dynamo `ConditionCheck`). So a signal
  delivered before the timeout commits always wins, and one that raced the deadline is re-consumed on the next
  tick instead of being silently dropped — no orphaned signals, on any of the 8 backends.

  New public surface: `SignalOutcome<T>`, `Outbox.requireVersion` (a checkpoint precondition), and
  `CheckpointResult` (checkpointStep's return type, which carries the guard result off the persisted-memo shape).

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

## 2.0.0-alpha.3

### Minor Changes

- 5b07ed6: First-user field-report fixes (v2 on Lambda + DynamoDB):

  - **A `try/catch` around `ctx.*` is now safe.** `ctx.sleep` / `ctx.signal` / `ctx.invoke` suspend the
    run by _throwing_ a control signal; a `catch` that swallowed it used to commit the next checkpoint
    at the wrong cursor and drift the run permanently. The engine now re-propagates a swallowed suspend
    at the next `ctx.*` call (and when the body returns), so the suspend still reaches the engine and
    the run parks + resumes correctly — you no longer have to special-case control signals in your own
    error handling.
  - **`StepPolicy.classify` gains the attempt number** — `(error, attempt) => "transient" | "permanent"`
    — and is now documented: fail fast on permanent (4xx/validation) errors instead of burning the
    in-invocation + run-level retry budget.

- acbe2bb: Checkpoint-based lease renewal (first-user field report #4).

  A long or many-step run advanced its whole flow body under the single claim-time lease, so `leaseMs`
  had to exceed the longest run's wall-clock or a slow run got its lease stolen and double-executed. The
  executor now renews the lease (`queue.heartbeat`) as the run commits steps — best-effort, and only
  once the lease is half-consumed so quick steps don't each cost a heartbeat write. Long multi-step runs
  are safe instead of banned by the "`leaseMs` > longest run" convention. Backend-agnostic — uses the
  existing `heartbeat` port method; no backend changes.

- 2257a3e: Self-scheduling serverless (`nextWakeAt`) — first-user field-report feature.

  A cron-cadence serverless driver advances a `ctx.sleep(15s)` only at the cron floor (1 minute on
  AWS). Now `serverlessTick`'s `SweepResult` carries **`nextWakeAt`** — the earliest pending timer
  (sleep / retry / cron) after the tick drained the due ones — and **`engine.nextWakeAt()`** exposes
  the horizon standalone, both backed by a new **`Timer.nextDueAt(now)`** port method (one bounded read
  on each backend's due-ordered index, never a scan). A driver arms a one-shot (EventBridge Scheduler /
  SQS `DelaySeconds` / Step Functions `Wait`) for exactly `nextWakeAt` and pays nothing while idle, so
  cost scales with pending work instead of wall-clock. `nextWakeAt` is timer-only; signals and
  child-joins wake by a push on submit/signal. Additive — fixed-cadence drivers and `engine.run()` are
  unaffected.

- 12f3baa: Structured `TickResult` (first-user field report #5).

  `serverlessTick` / `tickOnce` / `engine.tick()` reported a bare status string per run, so a driver
  seeing `["flow_drift", "failed"]` had to query the store to learn WHICH run and WHY. `TickResult` is
  now `{ runId, status, error?, cursorKey? }` — a failed, retrying, or drifted tick carries the error
  (and, for a drift, the cursor key it drifted at), so a serverless `SweepResult` consumer can log/route
  it without touching the store. The status-string union is now exported as `TickStatus`.

  Note: this is a breaking shape change for code that compared a tick result as a string
  (`result === "done"`) — read `result.status` instead.

### Patch Changes

- 539a1c2: Recovery & operations guide (first-user field report #3).

  The field report asked for a supported heal/repair path for a stuck run, or at least documented
  recovery. Since a `try/catch` around `ctx.*` is now safe (field report #1), the main way a run drifted
  permanently is gone — so rather than a risky memo-clearing "heal" primitive, the recovery is composing
  the existing levers. `docs/v2/RECOVERY.md` is now the lever-by-scenario guide: `retry` for a transient
  failure, `park` + redeploy or a version bump for drift (keep old versions registered until in-flight
  runs drain), and `cancel` + a fresh submit (new idempotency key — re-using the key returns the existing
  run, not a fresh one) for an un-resumable run. Linked from the core README.

## 2.0.0-alpha.2

### Minor Changes

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

- 3377316: Fan-out, structured concurrency, and an idempotency policy:

  - **Fan-out + join**: `ctx.invoke` now takes one child or many — `ctx.invoke([{ flow, input }, …])` spawns
    every child in parallel and joins on all of them, resolving with the outputs in order (per-spec typed).
    Children spawn in chunks, each an atomic memoized checkpoint, so a fan-out is crash-safe on every
    backend (no unrecoverable DynamoDB two-phase overflow).
  - **Fast-fail + first-class failure cascade**: if any fan-out child fails or is cancelled, the parent
    fails immediately and its still-running siblings are cancelled. More broadly, cancellation now cascades
    to non-terminal descendants on **any** non-success termination — an explicit `cancelRun` _and_ a plain
    failure (previously only explicit cancel cascaded; a failed parent left its children running).
  - **`onDuplicate` idempotency policy**: `submit(..., { idempotencyKey, onDuplicate })` — `"reuse"`
    (default) returns the existing run's handle on a key hit; `"error"` throws `DuplicateRunError`
    (code `RUN_DUPLICATE`) so an accidental double-submit surfaces instead of silently collapsing.

  See `docs/v2/CONTRACTS.md`.

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

### Patch Changes

- a624058: Declare `license: MIT` and the repository field in every package manifest — the alpha.1 tarballs showed as "Proprietary" on npm.

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
