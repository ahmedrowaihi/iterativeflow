---
"@iterativeflow/sqlite": minor
---

New backend: **`@iterativeflow/sqlite`** — the four ports over one SQLite database, for embedded and
edge use (a local file, Turso/libSQL, or a Cloudflare Durable Object's SQLite storage) through a
small `Sql` driver seam. State and dispatch share the database, so the transactional outbox commits
in one `BEGIN…COMMIT` — the same atomic model as Postgres. Single-writer, so `claim` is a plain
`SELECT`+`UPDATE` (no `SKIP LOCKED`); Wakeup is in-process. Passes the same
store/queue/timer/wakeup/outbox/signal/reconcile/cron/engine conformance suites as the other
backends. `@libsql/client` is a peer dependency; a `libsqlDb` adapter is included.
