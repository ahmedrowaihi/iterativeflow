---
"@iterativeflow/core": minor
---

Bulk control: `engine.cancelMany(filter, limit)` and `engine.retryMany(filter, limit)`.

Bulk creation (`submitMany`) and bulk deletion (`purge`) existed; control did not, so abandoning a
queue or re-driving a failure wave meant paging ids and issuing one round trip per run — and
`cancelRun` walks `childrenOf` recursively, so it was well over N. A downstream deployment reported
~11k failed runs of one flow and had built a sweep with a page cap, a time budget and a `seen` set to
work around it.

Both take the same `RunFilter` the ops UI already passes to `listRuns` (now with `version`), so "act
on what I filtered" needs no new vocabulary, and both mirror `deleteRuns`: narrow by filter, cap with
`limit`, return the count acted on, repeat until `< limit`. A filter with no predicate throws — a set
operation over everything has to be spelled out.

Cancel intersects with the live set and retry with `failed`, unconditionally, so neither can be
widened past the statuses it is defined on. Retry zeroes `attempts` per row, matching the single-run
fix, or a dead-lettered run would be re-failed without executing.

Bulk cancel deliberately does not walk descendants: a child whose parent went non-success cancels
itself on its next dispatch, and reconcile re-enqueues exactly those children, so the cascade still
completes — one maintenance interval later instead of inline. That is what keeps this one round trip
rather than a recursive graph walk.
