# MySQL

`@iterativeflow/mysql` runs the engine on MySQL (InnoDB). Use it when MySQL is already your database.
If you're choosing fresh for a server deployment, [Postgres](./postgres.md) has more features (event
timeline, push wake-ups, an error classifier).

## Requirements

- MySQL 8.0.1 or newer. The claim uses `SELECT ... FOR UPDATE SKIP LOCKED`, added in 8.0.1.
- InnoDB tables (the default). The engine relies on row locks.
- You provide a `mysql2` `Pool`. The library never opens connections on its own.
- Applying the schema needs `CREATE TABLE` rights; a running worker needs `SELECT`/`INSERT`/`UPDATE`/
  `DELETE`.

## Install

```bash
npm install @iterativeflow/mysql @iterativeflow/core mysql2
```

`mysql2` is a peer dependency (3.0 or newer).

## Set up the database

`applySchema` creates the engine's tables (idempotent — run it on every boot or once as a migration).

```ts
// db.ts
import { createPool } from "mysql2/promise";
import { mysqlPool, applySchema } from "@iterativeflow/mysql";

export const pool = createPool(process.env.DATABASE_URL!);
export const sql = mysqlPool(pool);

await applySchema(sql);
```

To run several engines in one database, pass a table prefix: `applySchema(sql, "flows_")` and
`createMysqlBackend(sql, { prefix: "flows_" })`. `ddl(prefix)` returns the raw SQL for your own
migration tool.

## Run a worker

```ts
// worker.ts
import { createMysqlBackend } from "@iterativeflow/mysql";
import { createEngine, defineFlow } from "@iterativeflow/core";
import { sql, pool } from "./db";

const backend = createMysqlBackend(sql);

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
await pool.end();
```

For serverless, call `serverlessTick(backend, registry([greet]), { batchMax: 20, leaseMs: 30_000 })`
per invocation instead of `engine.run()`.

## Operating in production

### Isolation and connection pooling

The engine runs each transaction at READ COMMITTED (not MySQL's REPEATABLE READ default), which it sets
per transaction. This avoids REPEATABLE READ's gap locks, which would block concurrent claims.

On **PlanetScale / Vitess**, a per-connection `SET` taints the pooled connection into a reserved slot
and can exhaust the pool. Turn the per-transaction `SET` off and set the server's default isolation to
READ COMMITTED instead:

```ts
export const sql = mysqlPool(pool, { setIsolation: false });
```

Give the engine its own pool; don't share it with your web app.

### Sharding contention

If you shard flows across workers by name, know that MySQL contends more than Postgres. The flow name
is on the `run` table, which the claim joins, and MySQL locks the eligible `job` rows before applying
the name filter — so the first worker to claim briefly locks rows another shard wants, which show up on
its next poll. Nothing is claimed twice or by the wrong shard; there's just more contention per round.
See [deployment](../deployment.md).

### Vitess: no foreign keys

On Vitess/PlanetScale, don't add foreign keys to tables next to the engine's. Vitess supports FKs only
on unsharded databases, and FK cascades aren't written to the binlog, which silently corrupts CDC and
replicas. The engine's own schema doesn't use cross-table FKs to the queue tables for this reason.

### Retention

Terminal runs stay until you delete them. Prune on a schedule; on Vitess this also keeps deletes small
(one big delete is a long transaction to avoid):

```ts
let removed: number;
do {
  removed = await engine.prune(7 * 24 * 60 * 60 * 1000, 500);
} while (removed === 500);
```

### Purge lag

MySQL's version of dead-row bloat is InnoDB purge lag: a long-running or idle-in-transaction session
stops purge and slows every table on the server. Keep transactions short (the engine already does) and
make sure your pool rolls back on return. Watch `trx_rseg_history_len` — over ~1M means purge is
blocked, usually by a stuck transaction elsewhere.

### Health checks and autoscaling

- `engine.health()` returns run counts per status; `engine.liveness()` returns the dispatch backlog and
  oldest-claimable age (use it for a readiness probe).
- For autoscaling, `engine.pendingWork(names?)` returns the backlog as one number. Serve it over HTTP
  (the dashboard's `GET /api/metrics`) for a KEDA `metrics-api` scaler. Unlike Postgres, MySQL has no
  in-database `pending_work()` function — use the HTTP path.

### Running a pool of workers

Workers judge lease expiry by their own clock — keep clocks NTP-synced and size `leaseMs` above your
longest step plus skew. See [deployment](../deployment.md).

## Optional features

### Committing your own data with a run

`inTx(pool, (backend, tx) => ...)` runs `submit`/`enqueue` and your own writes in one transaction.

There is no MySQL error classifier (that's Postgres-only). A step that throws a transient DB error
retries at the run level like any other throw.

## Troubleshooting

- **Claims block or deadlock under load.** You're on REPEATABLE READ (gap locks). The engine sets READ
  COMMITTED per transaction; if you set `setIsolation: false`, make the server default READ COMMITTED.
- **Connections exhausted on PlanetScale.** The per-transaction `SET` is creating reserved connections.
  Set `setIsolation: false` (above).
- **`SKIP LOCKED` syntax error.** MySQL is older than 8.0.1.
- **Claim latency climbs over time.** Purge lag — look for a long/idle transaction and prune completed
  runs.

## Other backends

Postgres, SQLite, MongoDB, DynamoDB, Redis, and Durable Objects each have their own guide in this
folder. Cross-cutting topics live in [deployment](../deployment.md).
