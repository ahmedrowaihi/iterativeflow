# @iterativeflow/mysql

A MySQL backend for [iterativeflow](https://github.com/ahmedrowaihi/iterativeflow) — the four ports
(Store / Queue / Timer / Wakeup) over one InnoDB database. State and dispatch share the database, so
the transactional outbox commits in one `BEGIN…COMMIT`, and `claim` uses `FOR UPDATE SKIP LOCKED` for
contention-free batch dispatch. Passes the same conformance suites as the Postgres, SQLite, DynamoDB,
and Redis backends. Requires MySQL 8+ (for `SKIP LOCKED`).

```ts
import { createPool } from "mysql2/promise";
import { createMysqlBackend, applySchema, mysqlPool } from "@iterativeflow/mysql";
import { createEngine } from "@iterativeflow/core";

const sql = mysqlPool(createPool(process.env.MYSQL_URL));
await applySchema(sql);
const engine = createEngine(createMysqlBackend(sql), [
  /* your flows */
]);
```

## Notes

- **`mysql2` is a peer dependency.**
- **Wakeup is in-process** (like SQLite/DynamoDB/Redis).
- **No `RETURNING`**: first-writer-wins reads `affectedRows` (`INSERT IGNORE`), and `claim` does
  `SELECT … FOR UPDATE SKIP LOCKED` then `UPDATE` — the semantics are identical to the Postgres
  backend, just expressed without `RETURNING`.
- Timestamps are BIGINT epoch ms; JSON columns are LONGTEXT. Run `applySchema(sql, prefix?)` once.
