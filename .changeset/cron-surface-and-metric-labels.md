---
"@iterativeflow/core": minor
---

Crons are readable and removable, and metrics carry the flow they belong to.

**`engine.listCrons()` / `engine.removeCron(name)`.** The cron surface was write-only — `upsertCron`,
`dueCrons`, `dueCronCount`, `advanceCron` — with no way to see what was registered or to retire one.
Because registration is an upsert, deleting a cron from your source did nothing: the row outlived it
and kept firing forever, and the only fix was raw SQL against the table. Both are on the `Store` port
and implemented by all 8 backends, with a conformance case.

**`Metrics` callbacks now carry a flow label.** They passed only a `runId`, so labelling a metric by
flow meant a store read inside the callback and per-flow p95 or failure-rate was effectively
unobtainable. `runStarted`, `runSettled` and `runSuspended` now receive `{ name, version }`;
`runSettled` also gets `durationMs` (wall-clock from the run's creation) and `errorCode`, and
`stepFinished` gets its own `durationMs`. Existing callbacks keep working — the additions are trailing
parameters.
