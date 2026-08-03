# v2 adversarial review — decisions log

Findings from the adversarial review of the transactional-outbox seam (the correctness core).
Applied fixes are green in the conformance suites; deferred items are gated to the phase that
can actually close them. Nothing here is dropped — each has an owner phase.

## Applied (fixed + conformance-tested)

- **C1 — `markRunning` could resurrect a terminal run.** A stale timer firing after a cancel
  re-enqueued the run; `markRunning` flipped `canceled → running` and the flow re-completed,
  re-firing terminal effects. Fixed: `markRunning` is a no-op on a terminal run (returns
  attempts unchanged). Test: `store` → "markRunning never resurrects a terminal run".
- **H5 — no atomic timer-cancel on completion.** A run completing after an `awaitSignal` with a
  timeout left the timeout timer pending, which later re-dispatched the terminal run. Fixed:
  `Outbox.cancelTimers` clears timers in the same write as the terminal. Test: `outbox` →
  "markTerminal cancels a pending timer atomically".
- **M2 — `ctx.invoke` trusted its locally-generated childId.** Under a concurrent first-writer
  race the checkpoint returns the _winner's_ childId; the loser would await a child it never
  created. Fixed: `invoke` uses the returned `StepOutcome.result` as authoritative.
- **M4 — no fast lease release.** Added `Queue.release` (CAS-gated) so a run yields/retries
  without waiting out `leaseMs`. Tests: `queue` → release re-claimable + stale-release no-op.
- **M7 — doc overclaimed exactly-once.** `checkpointStep` JSDoc now states exactly-once _memo_,
  at-least-once _effect_ (fn runs before the checkpoint; a pre-commit crash re-runs it).
- **H1 (already closed in the executor)** — waits ARE memoized: `ctx.sleep`/`sleepUntil`
  checkpoint the absolute deadline as a step, so replay compares against the stored instant and
  re-suspends to the _same_ time (no drift, no early-wake restart under a short lease).

## Deferred — gated to the phase that can close them

### Postgres/DynamoDB backend phase

- **C2 / H6 — atomicity + concurrent-checkpoint are untestable in single-threaded memory.**
  The outbox suite proves commit-together and skip-on-replay, but a backend that writes the step
  then the outbox in _separate_ statements would still pass. The Postgres backend MUST use one
  `WITH ins AS (INSERT … ON CONFLICT DO NOTHING RETURNING …) …` CTE (outbox gated on the CTE);
  Dynamo MUST use one `TransactWriteItems` with `attribute_not_exists` on the step item. Add
  backend-level tests: (a) fault-injected abort between step and outbox commits neither; (b) N
  concurrent `checkpointStep(sameKey, spawn)` → exactly one child. **Memory-green is necessary,
  not sufficient.**
- **H3 — Dynamo `TransactWriteItems` 100-item / 4MB cap.** Unbounded `Outbox.spawn` exceeds it
  (~40 children/step ceiling). Two-phase fan-out: the step records the childId _list_ (small);
  children are spawned lazily in follow-up ticks, each an idempotent insert-by-id. Document the
  per-outbox item budget as a backend contract; add a >100-child conformance test.
- **H4 — one transactional domain per Backend.** Store + Queue + Timer must share the same
  transactional substrate (same Postgres DB; same Dynamo transaction scope). EventBridge
  Scheduler as a timer CANNOT join a Dynamo transaction → relegated to a best-effort,
  non-outbox tier, not the durable-deadline path.
- **M3 — childId collision.** Define childId as globally-unique (UUID); child insert is
  conditional on all backends (`ON CONFLICT DO NOTHING` / `attribute_not_exists`). Decide: a
  collision with an _unrelated_ run is an error (fail the parent), not a silent idempotent drop.

### Engine-hardening phase

- **H2 — durable signal inbox.** External-signal flows (`ctx.awaitSignal(name)`) need a durable
  inbox: `Store.postSignal(runId, name, payload)` (committable via a caller outbox) + delivered
  signals surfaced in `RunSnapshot` so consumption memoizes. Wakeup stays best-effort; the inbox
  makes poll-first correct for signals, not just completion.
- **M1 — attempts conflation / poison-pill.** `markRunning` increments on every claim (including
  lease-expiry re-claims), so a worker-crashing step burns the retry budget without a deliberate
  retry, and an engine crash before the terminal write re-claims forever. Need a durable
  dead-letter cap enforced where attempts is incremented; consider separating dispatch-count
  from retry-count.
- **Multi-worker parent-wake backstop (reconciler).** The fast path — child `markTerminal`
  enqueues the parent atomically — has a lost-wakeup race in multi-worker: if the child completes
  between the parent's `loadRun(child)` and its suspend+ack, the child's enqueue can be erased by
  the parent's ack. A periodic reconciler that re-enqueues `awaiting_signal` parents whose awaited
  child is terminal closes it. Single-worker memory has no race (child runs on a later tick), so
  this is a Postgres/multi-worker concern. Needs the query surface below.

### Dashboard / ops phase

- **M5 — query surface.** `Store.listRuns(filter, page)` (status/tag/name) and
  `childrenOf(runId)` (parent index / GSI). The reconciler above also needs these.
- **M6 — caller-side transactional enqueue.** The 4-port abstraction hides the transaction
  handle, so a caller's business transaction can't atomically enqueue a workflow. Offer a
  backend-specific escape hatch (accept a pg client / a Dynamo transaction-item builder) or a
  documented intent-table + relay pattern. State the limitation explicitly.

## Cooperative-cancel contract (decided, documented here)

Cancel is **cooperative**. A worker mid-flow on a run that was just canceled keeps checkpointing
steps and firing their outboxes until its next boundary; `markRunning` (C1) stops the _next_
dispatch, not the in-flight one. A `checkpointStep` run-status guard could tighten this at the
cost of a read per step — deferred; the contract is "cancel stops future dispatch, in-flight
step effects may still land."
