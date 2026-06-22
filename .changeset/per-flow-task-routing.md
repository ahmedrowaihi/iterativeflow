---
"iterativeflow": major
---

Per-flow task routing: a worker now claims only the flows it registered.

Each flow is enqueued under its own graphile-worker task identifier
(`flow:run:<name>@<version>`) instead of a shared `flow:run`. graphile routes a
run only to workers whose task list contains that identifier, so splitting
workers by registered flow finally isolates them — a media worker can no longer
claim (and fail) a clone job. Mirrors how crons already route per name.

Breaking:

- `TxEnqueue`'s signature changed from `(tx, runId, opts?)` to
  `(tx, job: { runId; name; version }, opts?)`. Only affects consumers passing a
  custom `worker.enqueue`; the built-in graphile enqueue is unchanged for callers.
- The worker's task list is fixed when `engine.listen()` runs, so `engine.register`
  after `listen()` now throws (previously it silently produced runs no worker could
  claim). Register all flows before `listen()`.
