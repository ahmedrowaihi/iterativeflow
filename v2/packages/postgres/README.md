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

## Low-latency push (opt-in `LISTEN/NOTIFY`)

By default dispatch and `result()` are **poll-first** — connection-safe behind RDS
Proxy / PgBouncer and the only option that fits serverless. For a **resident /
multi-pod** deployment you can layer instant push on top, without pinning a
`LISTEN` connection on the serverless path:

```ts
import { applyNotifyTriggers, createPgListener, createPgBackend } from "@iterativeflow/postgres";

await applyNotifyTriggers(sql); // once, alongside applySchema — installs the two triggers

const listener = createPgListener(pool, { schema: "workflow" });
listener.start(); // one dedicated LISTEN connection, reconnects with backoff

const backend = createPgBackend(sql, { listener }); // wires BOTH push seams off the one listener
const engine = createEngine(backend, flows);
engine.run(); // claim loop wakes on enqueue; result() waiters wake on completion — no extra wiring
```

Two DB triggers do the signalling: a per-statement `job`-insert trigger fires a
`wake` NOTIFY on **every** enqueue — including the transactional-outbox ones
(`ctx.invoke` spawn, `engine.signal`'s re-enqueue) that never pass through
`queue.enqueue` — and a `run`-terminal trigger fires a `done` NOTIFY. One
`createPgListener` connection multiplexes both. It's purely a latency
optimization: a missed notify costs one poll tick, never correctness, so keep a
sane `tickMs` as the backstop. Don't install the triggers on a serverless /
pooled deployment that can't hold the `LISTEN` connection.
