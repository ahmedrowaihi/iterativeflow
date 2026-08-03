---
"@iterativeflow/mongodb": minor
---

`inTx` for MongoDB — completing transactional-enqueue across all tx-capable backends.

`@iterativeflow/mongodb` now exports `inTx(client, fn, opts?)`: it runs `fn` in one transaction with a
`Backend` bound to a caller session, so a `submit` (startRun + enqueue) commits atomically with the
caller's own writes — the flow runs iff the domain row commits, and a throw rolls back both. The bound
session threads through the submit write path; the store's internal transaction helper runs inline in
the caller's session (MongoDB can't nest transactions). Requires a replica set. Only the write path
joins the transaction — reads via the bound backend do not observe its uncommitted writes (unlike a SQL
read-your-own-write). DynamoDB and Redis still can't offer this (no caller-joinable transaction).
