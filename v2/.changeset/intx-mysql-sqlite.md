---
"@iterativeflow/mysql": minor
"@iterativeflow/sqlite": minor
---

`inTx` for MySQL and SQLite — transactional-enqueue parity with Postgres.

`@iterativeflow/mysql` and `@iterativeflow/sqlite` now export `inTx(driver, fn, opts?)`, mirroring
`@iterativeflow/postgres`: it runs `fn` in one transaction with a `Backend` bound to it, so a `submit`
(startRun + enqueue) commits atomically with the caller's own writes — the flow runs iff the domain row
commits, and a throw rolls back both. It stays poll-first (a short bounded tx, not a pinned `LISTEN`)
and closes only the enqueue-drop window (the reconciler still recovers a worker dying mid-flow). Not in
core `submit` because it needs a caller-joinable transaction — DynamoDB and Redis can't offer it. Also
documents the pattern in the postgres README.
