# @iterativeflow/sqlite

A SQLite backend for [iterativeflow](https://github.com/ahmedrowaihi/iterativeflow) — the four ports
(Store / Queue / Timer / Wakeup) over one SQLite database. Embedded and edge: a **local file**,
**[Turso](https://turso.tech)/libSQL**, or a **Cloudflare Durable Object's** SQLite storage, all
through the same `Sql` seam.

State and dispatch share the database, so the transactional outbox commits in one `BEGIN…COMMIT` —
the same atomic model as the Postgres backend, no Lua or two-phase choreography. Passes the same
conformance suites as the Postgres, DynamoDB, and Redis backends.

```ts
import { createClient } from "@libsql/client";
import { createSqliteBackend, applySchema, libsqlDb } from "@iterativeflow/sqlite";
import { createEngine } from "@iterativeflow/core";

const sql = libsqlDb(createClient({ url: "file:workflow.db" })); // or a Turso url
await applySchema(sql);
const engine = createEngine(createSqliteBackend(sql), [
  /* your flows */
]);
```

## Notes

- **`@libsql/client` is a peer dependency** — bring your own client (a local file, `:memory:` won't
  share across transactions, or a Turso remote). Any adapter satisfying the two-method `Sql` interface
  works, so a Durable Object's `ctx.storage.sql` can drive the same backend.
- **Wakeup is in-process** (like DynamoDB/Redis); a run completing in another process is caught by the
  poll-first `result()`.
- **Single-writer**: SQLite serializes writes, so `claim` runs a plain `SELECT`+`UPDATE` in a
  transaction — no `SKIP LOCKED` needed.
- Run `applySchema(sql, prefix?)` once before use.
