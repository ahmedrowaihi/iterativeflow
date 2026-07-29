# @iterativeflow/memory

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
