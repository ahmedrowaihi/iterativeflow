---
"@iterativeflow/core": minor
---

`pollTimeoutMs` — bound the resident loop's DB poll so a dead Postgres connection can't silently freeze it.

The resident worker loop does one `await` per cycle on the DB poll (drain due timers + claim a batch). A
dropped/black-holed connection — RDS failover, PgBouncer killing a pinned socket — leaves that query
awaiting a dead socket forever: the process stays alive but stops doing work, with no error. `tickOnce` /
`engine.run` now bound the poll with `pollTimeoutMs` (default 30s via `createEngine`; `0` disables — an
in-memory backend never hangs). On timeout the poll rejects `PollTimeoutError`; the resident loop already
catches tick errors, so it logs via `observe.metrics.tickError` and re-polls on a fresh pooled connection.
Bounds the poll only, never step execution.
