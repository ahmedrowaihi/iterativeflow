---
"@iterativeflow/core": minor
---

New `EngineOpts.reconcileLimit` (default 100): how many crash-stranded runs a reconcile sweep
re-drives. It used to borrow `batchMax`, so lowering the claim batch — the usual tuning when each run
holds a worker for a long time — silently throttled crash recovery to that same number per sweep. A
worker on `batchMax: 1` recovered one stranded run per maintenance cycle. It applies to the resident
loop's maintenance sweep, `engine.reconcile()` and `serverlessTick` alike.

Each re-drive is a concurrent enqueue, so this also bounds the sweep's fan-out per worker per cycle.

Also corrects `EngineOpts.leaseMs`, which said "There is no heartbeat, so it must exceed the longest
step's wall-clock duration". The lease is renewed as a run commits steps, and continuously while a
step declaring `StepPolicy.timeoutMs` runs — so the floor is the longest step that declares **no**
timeout. Sizing a lease to the longest step sets your crash-recovery time to that same duration,
which the old wording made look unavoidable.
