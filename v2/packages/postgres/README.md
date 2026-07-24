# @iterativeflow/postgres

Postgres [`Backend`](../core) for [iterativeflow](https://github.com/ahmedrowaihi/iterativeflow)
v2. The store, queue, and timer share one database, so a durable checkpoint
commits as a single `BEGIN…COMMIT` transactional outbox. Dispatch uses
`SELECT … FOR UPDATE SKIP LOCKED`; leases are CAS-guarded.

```bash
npm install @iterativeflow/postgres @iterativeflow/core pg
```

```ts
import { createEngine } from "@iterativeflow/core";
import { createPgBackend, pgPool, applySchema } from "@iterativeflow/postgres";
import { Pool } from "pg";

const sql = pgPool(new Pool({ connectionString: process.env.DATABASE_URL }));

await applySchema(sql); // idempotent; creates the `workflow` schema + tables
const engine = createEngine(createPgBackend(sql), [myFlow]);
```

Pass any `Sql` to `pgPool` — a `pg` `Pool` is the default adapter. Override the
schema name with `createPgBackend(sql, { schema })` (match it in `applySchema`).

## Schema ownership

| You want                               | Use                                                           |
| -------------------------------------- | ------------------------------------------------------------- |
| Zero setup (dev, tests, single owner)  | `applySchema(sql, schema?)` — idempotent on boot              |
| The raw DDL string                     | `ddl(schema?)`                                                |
| A drizzle schema **you** own + migrate | `drizzleSchema(schema)` or the `iterativeflow-pg-drizzle` bin |

```bash
# Emit a standalone drizzle schema file you migrate with drizzle-kit:
npx iterativeflow-pg-drizzle src/db/iterativeflow.schema.ts --schema workflow
```

See [docs/v2/MIGRATION.md](../../../docs/v2/MIGRATION.md) for the drizzle route
and serverless notes.
