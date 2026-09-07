---
"@iterativeflow/core": minor
---

Filtered retention: `Store.deleteRuns(filter, limit)` + `engine.purge(filter, limit)`.

`deleteRunsOlderThan` could only sweep by age, so the history a mass cancel leaves behind (thousands
of `canceled` runs of one flow) sat inflating every dashboard count until the retention window caught
up. `deleteRuns` narrows the same sweep by `before`, `name`, `version` and terminal `status`, in the
same single transaction and the same cascade order, returning the count deleted so callers batch
until `< limit`. `deleteRunsOlderThan` stays as the `{ before }` case — one delegating line per
backend — and `engine.prune` is unchanged.

The terminal-only guard is unconditional: the filter narrows within `done`/`failed`/`canceled` and
can never widen onto a live run, and a filter with no predicate at all is refused, since "delete all
history" should be spelled `{ before: new Date() }`.

New on the backend SPI, in `#purge` and shaped after the existing `isOrphaned` / `orphanedRunsSql`
pair: `purgeStatuses` (the terminal intersection + empty-filter refusal), `purgeMatcher` (the row
predicate the scanning backends filter with) and `purgeWhereSql` (the same predicate as a `WHERE`
body + binds for the SQL backends). `TERMINAL_STATUSES` moves next to `RUN_STATUSES` in `#types` so
the new `TerminalStatus` type derives from it rather than restating the list.

Implemented across all 7 backends and covered by a store conformance case.
