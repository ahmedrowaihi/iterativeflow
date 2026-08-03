---
"@iterativeflow/core": minor
---

Fan-out, structured concurrency, and an idempotency policy:

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
