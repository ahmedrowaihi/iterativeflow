# SQLite

`@iterativeflow/sqlite` runs the engine on SQLite — a local file, Turso/libSQL, or (through a separate
adapter) React Native via op-sqlite. Use it for a single node, embedded apps, and on-device work.

SQLite is **single-writer**: one writer at a time, no matter how many workers. It suits a single
process (or a moderate write rate on one machine), not a pool of machines contending to write. For
that, use [Postgres](./postgres.md) or [MySQL](./mysql.md).

## Requirements

- A `@libsql/client` `Client` (local file, or a Turso URL). For React Native, use the op-sqlite adapter
  (below).
- The database lives on one machine. WAL uses shared memory, so workers must share the file locally —
  not over a network filesystem.

## Install

```bash
npm install @iterativeflow/sqlite @iterativeflow/core @libsql/client
```

`@libsql/client` is a peer dependency (0.14 or newer). For React Native, install
`@op-engineering/op-sqlite` instead.

## Set up the database

`applySchema` creates the tables and sets safe defaults for a durable file store: WAL mode (readers and
writers proceed together), `busy_timeout=5000` (retry on lock contention instead of erroring), and
`synchronous=NORMAL` (the WAL speed/durability sweet spot). It's idempotent.

```ts
// db.ts
import { createClient } from "@libsql/client";
import { libsqlDb, applySchema } from "@iterativeflow/sqlite";

export const client = createClient({ url: "file:iterativeflow.db" });
export const sql = libsqlDb(client);

await applySchema(sql);
```

`synchronous=NORMAL` can roll back the last few commits on a power loss or hard reboot (not corruption —
lost recent writes). If you can't afford that, set `PRAGMA synchronous=FULL` yourself after
`applySchema`. To run several engines in one file, pass a table prefix to `applySchema(sql, "flows_")`
and `createSqliteBackend(sql, { prefix: "flows_" })`.

## Run a worker

```ts
// worker.ts
import { createSqliteBackend } from "@iterativeflow/sqlite";
import { createEngine, defineFlow } from "@iterativeflow/core";
import { sql } from "./db";

const backend = createSqliteBackend(sql);

const greet = defineFlow({
  name: "greet",
  version: 1,
  run: async (ctx, input: { name: string }) => `hi ${input.name}`,
});

const engine = createEngine(backend, [greet]);
const stop = engine.run();

await engine.submit(greet, { name: "world" });

// on shutdown:
await stop();
```

## React Native (op-sqlite)

For on-device durable execution, use the op-sqlite adapter. Open **one** database connection for the
app and reuse it — op-sqlite's multi-connection modes have been unsafe.

```ts
import { open } from "@op-engineering/op-sqlite";
import { opSqliteDb, createOpSqliteBackend, applySchema } from "@iterativeflow/sqlite";

const db = open({ name: "iterativeflow.db" });
const sql = opSqliteDb(db);
await applySchema(sql);

const backend = createOpSqliteBackend(sql);
```

Don't fire writes with `Promise.all` on op-sqlite — parallel writes overwrite SQLite's global
last-insert-id and can corrupt results. Run writes sequentially. The adapter already declares write
intent with `BEGIN IMMEDIATE` and retries on `SQLITE_BUSY`, which is required: a transaction that starts
as a read and later writes fails immediately regardless of `busy_timeout`, so the engine always begins
its transactions as writes.

## Operating in production

### One writer

All workers on the file serialize their writes. Adding workers does not add write throughput. If you
need concurrent claims across machines, this isn't the backend — the shared, concurrent
[sharded-pool](../deployment.md) model needs Postgres/MySQL/etc.

### Retention

Prune terminal runs on a schedule: `await engine.prune(7 * 24 * 60 * 60 * 1000)`. `DELETE` doesn't
shrink the file — run `VACUUM` in a maintenance window if the file grows (it takes an exclusive lock),
or set `PRAGMA auto_vacuum=INCREMENTAL` at database creation.

### Health checks

`engine.health()` returns run counts per status; `engine.liveness()` returns the dispatch backlog and
oldest-claimable age.

### Backup / durability

For disaster recovery of a local file, use [Litestream](https://litestream.io) — it streams the WAL to
S3. It's async backup, not high availability or read scale-out; the last few seconds of writes can be
lost on a hard failure.

### Turso / libSQL embedded replicas

Embedded replicas keep reads local but forward writes to the remote primary, so write latency is
network-bound and the single-writer model still applies. Don't open the local file directly while it's
syncing.

## Troubleshooting

- **`SQLITE_BUSY: database is locked` under concurrency.** You issued concurrent transactions on one
  connection (e.g. `Promise.all` of two claims). SQLite is single-writer — serialize writes.
- **`cannot start a transaction within a transaction`.** Same cause — one connection, one transaction
  at a time.
- **Lost recent writes after a crash.** `synchronous=NORMAL` trades this for speed; use `FULL` if you
  can't afford it.
- **The `.db` file keeps growing.** `DELETE`/prune doesn't shrink it; `VACUUM` or `auto_vacuum`.

## Other backends

Postgres, MySQL, MongoDB, DynamoDB, Redis, and Durable Objects each have their own guide in this
folder. Durable Objects run this same SQLite backend on Cloudflare storage — see
[durable-objects](./durable-objects.md). Cross-cutting topics live in [deployment](../deployment.md).
