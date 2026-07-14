---
"iterativeflow": minor
---

Add per-call transactional enqueue and atomic batch dispatch.

- `handle.start(input, { tx })` — pass a `tx` (your `WorkflowDb` transaction handle) and the run-row insert and the queue's add-job run through your transaction instead of the engine's own pool, so the run commits atomically with the rows that create the flow's subject. Nothing appears until you commit, and a rollback discards the run — eliminating both the enqueue-before-commit read race and the commit-then-crash orphan window. Omitting `tx` is unchanged.
- `handle.startMany(items, { tx? })` — insert and enqueue many runs atomically in a handful of statements (one multi-row insert for runs, one for their started events, one bulk enqueue) instead of one round-trip per run. All-or-nothing; pass `tx` to commit the batch with your own rows. Results are returned in input order, and idempotent duplicates (within a batch or across calls) return the original run.
