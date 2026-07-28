# Recovery & operations

How a run gets stuck, and the lever for each. There is no single "heal" button because the right
recovery depends on _why_ a run is stuck — a transient outage, a code change under a live run, or a
genuinely un-resumable run are three different problems.

## The levers

| Lever                            | What it does                                                                                                                                                  |
| -------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `engine.retry(runId)`            | Re-drive a **failed** run. Completed (`ok`) step memos are kept, so replay skips them and only the work after the failure re-runs. No-op on a non-failed run. |
| `engine.cancel(runId)`           | Take a run terminal (`canceled`), cascading to its children (structured concurrency), and clear its pending timer. The abandon lever.                         |
| `driftPolicy` (`park` \| `fail`) | On a flow-shape drift, `park` re-checks on a timer (recovers once the code matches again) instead of failing. `fail` dead-letters it as `FLOW_DRIFT`.         |
| Keep old versions registered     | During a rollout, register both the old and new `(name, version)` so in-flight old-version runs still resolve and finish.                                     |

## Scenarios

### Transient failure (a step threw on a 5xx / timeout)

`engine.retry(runId)` re-drives it, skipping the memoized steps that already succeeded. For steps that
should _never_ retry (a 4xx / validation error), use `StepPolicy.classify` to fail fast instead of
burning the retry budget — see the core README.

### A flow's shape changed under a live run (drift)

Replay compares each `ctx.*` call to the memo; a body that was reordered/refactored under a running
run trips the drift guard and applies the flow's `driftPolicy`:

- **`park` (default)** — the run parks (`retrying`) and re-checks on a timer. If you redeploy a fix
  that makes the shape match the memo again **at the same version**, the parked run resumes on its
  next wake with no further action. This is the common recovery.
- **A genuine shape change needs a version bump.** If the new body legitimately issues different
  steps, bump `.version(N)` — new submits run the new code. **Keep the old version registered until
  its in-flight runs drain**, otherwise an old-version run finds no registered flow (`unknown_flow`)
  and parks until you cancel it.

> Swallowing a durable suspend used to be the main way a run drifted permanently. That footgun is
> gone — a `try/catch` around `ctx.*` is now safe (the suspend re-propagates), so drift is almost
> always a real code change, recoverable by the two paths above.

### A genuinely un-resumable run

If a run cannot be made to replay (e.g. its input is now invalid, or you deliberately want to start
over), **cancel it and submit a fresh run**:

```ts
await engine.cancel(runId);
const fresh = await engine.submit(flow, input); // a NEW run
```

Note the idempotency subtlety: **re-submitting with the _same_ `idempotencyKey` returns the existing
(now-canceled) run, not a fresh one** — idempotency dedups by key, it is not a "retry." To start over,
submit with a new key (or none). Prefer this over forcing a re-run of a run whose memos are the
problem: a fresh run has a clean cursor and its steps re-run under the normal at-least-once contract.

### A poison-pill run (crash loop)

A run that crashes the worker uncatchably (not a caught error) is bounded by the retry policy's
`maxAttempts` (default 10): once a run's attempts exceed the cap it dead-letters as
`RUN_ATTEMPTS_EXHAUSTED` instead of re-claiming forever. Fix the cause, then `engine.retry(runId)`.

## Observability

A serverless driver doesn't need to query the store to learn _why_ a run stalled: each
`SweepResult.results` entry is `{ runId, status, error?, cursorKey? }` — a failed, retrying, or
drifted tick carries its error (and, for a drift, the cursor key it drifted at). Log/route on that.
