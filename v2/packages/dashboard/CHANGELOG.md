# @iterativeflow/dashboard

## 2.0.0-alpha.2

### Patch Changes

- a624058: Declare `license: MIT` and the repository field in every package manifest — the alpha.1 tarballs showed as "Proprietary" on npm.
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
