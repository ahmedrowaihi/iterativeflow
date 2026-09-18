---
"@iterativeflow/core": minor
---

New `Queue.enqueueMany(requests)` on the backend port, used everywhere the engine re-drives a batch:
`drainTimers`, `reconcile`, `submitMany`, each backend's outbox commit, and `retryRuns`.

Those paths issued one round trip per run. `retryRuns` was the worst — N sequential inserts inside an
open transaction, so lock hold time scaled with the batch times the network round trip. Measured on
Postgres: 500 runs took 102ms sequentially in a transaction, 6ms as one statement, and a 500-run
concurrent fan-out took 49ms while opening 500 pool connections the worker also needs for its steps.

Duplicate `runId`s in one call collapse to a single upsert, last wins. `distinctEnqueues` is exported
for backend authors.

**Breaking, for backend authors only.** `Queue` implementations must add `enqueueMany`, and
`EnqueueRequest` now comes from the queue port rather than the outbox port — it is the unit of both.
Applications calling `engine.*` are unaffected.
