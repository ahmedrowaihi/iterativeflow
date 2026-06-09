---
"iterativeflow": minor
---

Add `engine.retry(runId)` — replay a `failed` run from the step that failed. Memoized `ok` step results are preserved; the `failed_terminal` step row is deleted, the run is reset to `pending` with `attempts=0`, a `resumed` event is recorded, and the run is re-enqueued atomically. Returns a `RetryResult` discriminated by `kind`: `"queued"`, `"missing"`, or `"not_failed"` (with the current status).

This is **replay**, not restart: a fresh `handle.start(input)` is a brand-new run and re-executes every step. `engine.retry(runId)` resumes the same `runId` and skips work already done.
