---
"@iterativeflow/core": patch
---

Make a filtered purge use an index instead of scanning the run table.

Measured on Postgres 17 against 500k runs with 780 matching rows, using the shipped query: **217 ms
and 38,181 rows discarded, down to 1.87 ms with none discarded.** Three changes, all needed together.

- **A partial index with `status` in the key** — `(name, status, created_at) WHERE status IN
('done','failed','canceled')`, on Postgres and SQLite. The obvious index `(name, version,
created_at)` with the statuses only in the _predicate_ measured **worse than no index at all**: the
  predicate decides which rows are in the index, it does not let the planner seek one terminal
  status. Partial keeps live runs out entirely, so a row enters the index once, when it goes terminal,
  and the hot path never pays. MySQL has no partial indexes, so it is deliberately left alone rather
  than given a full index that would pay on every insert and transition.
- **Statuses render as SQL literals, not binds.** A partial index is only usable when the planner can
  prove the query's predicate implies the index's, and it cannot do that through a bind parameter —
  under a generic plan (which Postgres switches to after roughly five executions of a prepared
  statement) it silently drops the index and scans everything. That is the nastiest shape of bug:
  fast in tests and for the first minutes after deploy, then quietly not. The values come from the
  engine's own closed enum via `purgeStatuses`, never from a caller, and the SQL backends already
  render status tuples as literals everywhere else — the purge path was the deviation.
- **`ORDER BY` only when there is an age cutoff.** Without one the whole matched set is going
  eventually, so the order is arbitrary, and sorting forced a full read of every matching row before
  `LIMIT` could bound anything — which undercut the "repeat until `< limit`" contract by re-paying the
  scan on every batch. Backends already disagreed on the column (`created_at` vs `seq`) and
  conformance only ever asserted counts, so nothing promised an order.
