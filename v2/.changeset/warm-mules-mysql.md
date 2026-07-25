---
"@iterativeflow/mysql": minor
---

New backend: **`@iterativeflow/mysql`** — the four ports over one InnoDB database. State and dispatch
share the database, so the transactional outbox commits in one `BEGIN…COMMIT` and `claim` uses
`FOR UPDATE SKIP LOCKED`. Because MySQL has no `RETURNING`, first-writer-wins reads `affectedRows`
(`INSERT IGNORE`) and `claim` is a `SELECT … FOR UPDATE SKIP LOCKED` + `UPDATE` — same semantics as
the Postgres backend. Timestamps are BIGINT epoch ms, JSON is LONGTEXT, insertion order is an
`AUTO_INCREMENT` seq. Wakeup is in-process. Passes all nine conformance suites. Requires MySQL 8+;
`mysql2` is a peer dependency.
