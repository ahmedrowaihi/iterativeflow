---
"@iterativeflow/core": patch
---

Fix: `engine.retry()` did nothing for a run that dead-lettered — the one population you retry.

`retryRun` reset `status` and `error` but left `attempts` at its spent value. The executor checks the
dead-letter cap on the next claim, _before_ running the body: `markRunning` bumps attempts past
`maxAttempts` and the run is failed terminally with `RUN_ATTEMPTS_EXHAUSTED` without executing
anything. So retrying a run that had used its retry budget reported `retried: true`, then silently
re-failed it — with a different error code than the one it originally failed with.

Retry now zeroes `attempts` in the same atomic write, on all 7 backends. This matches how
`suspendRun` already treats the counter: a forward-progress park zeroes it too, because it counts
_no-progress_ re-claims, not legitimate resumes.

The old behavior was invisible because the regression test failed a run under `maxAttempts: 1` and
then re-drove it under the default `10`. Production uses one policy for both. The new tests use a
single policy end to end, plus a store-conformance case that pins the counter directly for every
backend.
