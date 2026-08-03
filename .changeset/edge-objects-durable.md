---
"@iterativeflow/durable-objects": minor
---

New: **`@iterativeflow/durable-objects`** — run iterativeflow inside a Cloudflare Durable Object on
its built-in SQLite storage. It's the `@iterativeflow/sqlite` backend driven through a thin `Sql`
adapter over `ctx.storage.sql` (`createDurableObjectBackend(storage)` + `applySchema`), so one DO
becomes a self-contained, strongly-consistent durable-execution engine at the edge with no external
database. No dependency beyond core + sqlite (the `SqlStorage` type is structural). Passes the same
nine conformance suites as every other backend, verified against Node's synchronous `node:sqlite`,
which matches the DO storage shape. A DO serves one request at a time (single-writer by
construction); the outbox relies on the DO's invocation-level atomicity rather than a manual
transaction, which DO SQLite forbids.
