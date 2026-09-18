---
"@iterativeflow/core": minor
---

Fixes for defaults that failed silently. Most are behaviour changes, listed with what to check.

- **A drift-parked run no longer dead-letters.** The default `driftPolicy: "park"` spent one retry
  attempt per re-check, so a drifted run failed with `RUN_ATTEMPTS_EXHAUSTED` about 10 seconds later —
  well before any redeploy could land. Parked runs now use a new `parked` status, keep their attempts,
  and re-check every 30 seconds until a fix deploys. `listRuns({ status: "parked" })` finds them. If you
  count or filter runs by status, add `parked`.
- **A run late in a claimed batch no longer runs twice.** A worker claims its batch at once and runs it
  one run at a time, so a run near the end could outlive its lease while waiting, then execute
  alongside the peer that re-claimed it. Each run now renews its lease before it starts, and skips
  itself (tick status `lease_lost`) if a peer already holds it.
- **Cancelling a finished run no longer erases it.** It overwrote a `done` run's output or a `failed`
  run's error. A run that is already terminal is now left as it is.
- **An event sink without a `level` now records events.** `level` defaulted to `"off"`, so wiring only
  `observe.sink` — as the Postgres guide shows — recorded nothing. It now defaults to `"all"`.
- **Worker-loop and sink errors go to `console.error`** when no `metrics.tickError` hook is set.
  Previously they vanished, so a broken database connection produced no output at all.
- **DynamoDB orders negative priorities correctly.** `-3` was claimed before `-5`.
- `RunLoopOpts.waitForWork` now receives the loop's `AbortSignal`, so a custom waiter can be
  interrupted by `pause()` and `stop()`.
