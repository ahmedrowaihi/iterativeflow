---
"@iterativeflow/core": minor
"@iterativeflow/postgres": minor
---

FlowError.cause capture + a Postgres classify preset (production field report — vod-media-convert).

- **`FlowError.cause`** — `toFlowError` now walks a thrown error's `.cause` chain (bounded depth) and
  flattens it into the persisted error, so a wrapper like `DrizzleQueryError` (generic "Failed query:
  rollback", the real pg error on `.cause`) no longer reduces a run record to `[object Object]`. This
  removes the need for a `failNormalized`/`dbStep` workaround.
- **`pgClassify`** (`@iterativeflow/postgres`) — a ready `StepPolicy.classify` preset: constraint, data,
  and syntax/access errors are permanent (fail fast), while connection drops, statement timeouts,
  deadlocks, and serialization failures stay transient (retry). Walks the `.cause` chain for the SQLSTATE.
- **Docs** — an error-sink recipe (`observe.sink` capturing `FlowError.cause`), idempotent-step guidance,
  and a note that `maxAttempts` already bounds a stalled-step reclaim loop (no blind re-dispatch).
