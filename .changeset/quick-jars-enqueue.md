---
"iterativeflow": minor
---

Add per-call transactional enqueue via `StartOpts.tx`. Pass a `tx` (your `WorkflowDb` transaction handle) to `handle.start(input, { tx })` and the run-row insert and the queue's add-job run through your transaction instead of the engine's own pool — so the run commits atomically with the rows that create the flow's subject. Nothing appears until you commit, and a rollback discards the run, eliminating both the enqueue-before-commit read race and the commit-then-crash orphan window. Omitting `tx` is unchanged.
