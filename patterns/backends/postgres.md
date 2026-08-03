# Postgres

`@iterativeflow/postgres` runs the engine on Postgres. It supports the most features of any backend:
a durable event timeline, `LISTEN/NOTIFY` wake-ups, a Postgres error classifier for step retries, and
an autoscaling query. Use it for a server deployment.

## Requirements

- Tested on Postgres 16. The schema uses identity columns and `SELECT ... FOR UPDATE SKIP LOCKED`,
  which need Postgres 10 or newer.
- You provide a `pg.Pool`. The library never opens connections on its own.
- Applying the schema needs `CREATE TABLE` rights. A running worker needs only `SELECT`, `INSERT`,
  `UPDATE`, and `DELETE` on the tables — grant those separately if you split roles.

## Install

```bash
npm install @iterativeflow/postgres @iterativeflow/core pg
```

`pg` is a peer dependency (8.10 or newer).

## Set up the database

`applySchema` creates the engine's tables (`run`, `step`, `job`, `timer`, `signal`, `event`, `cron`)
and the `pending_work()` function in a schema, `workflow` by default. It is idempotent — run it on
every boot, or once as a migration.

```ts
// db.ts
import { Pool } from "pg";
import { pgPool, applySchema } from "@iterativeflow/postgres";

export const pool = new Pool({ connectionString: process.env.DATABASE_URL });
export const sql = pgPool(pool);

await applySchema(sql);
```

To use a different schema, pass it here and to `createPgBackend`: `applySchema(sql, "flows")`. To run
migrations through your own tool instead, `ddl(schema)` returns the SQL string, and `drizzleSchema`
(or the `iterativeflow-pg-drizzle` CLI) emits a Drizzle schema.

## Run a worker

```ts
// worker.ts
import { createPgBackend } from "@iterativeflow/postgres";
import { createEngine, defineFlow, submit } from "@iterativeflow/core";
import { sql, pool } from "./db";

const backend = createPgBackend(sql); // pass { schema: "flows" } if you changed it

const greet = defineFlow({
  name: "greet",
  version: 1,
  run: async (ctx, input: { name: string }) => `hi ${input.name}`,
});

const engine = createEngine(backend, [greet]);
const stop = engine.run(); // claims and runs work until you call stop()

await engine.submit(greet, { name: "world" });

// on shutdown:
await stop();
await pool.end();
```

For a serverless worker (Lambda, a cron container), call `serverlessTick` once per invocation instead
of `engine.run()`. It takes a registry and requires both `batchMax` and `leaseMs`:

```ts
import { serverlessTick, registry } from "@iterativeflow/core";

const flows = registry([greet]);
const { nextWakeAt } = await serverlessTick(backend, flows, { batchMax: 20, leaseMs: 30_000 });
// nextWakeAt is the next time a timer or cron is due — schedule the next invocation for then
```

## Operating in production

### Connection pooling

Works behind RDS Proxy, PgBouncer transaction mode, and PlanetScale-style poolers, and on Neon and
Aurora Serverless v2. The backend uses `$1` params (nothing pins a prepared statement) and holds a
connection only for one step's write. Give the engine its own pool — if you share a pool with your web
app, request traffic can starve the workers. See [deployment](../deployment.md) for the full notes.

### Retention

Terminal runs stay in the `run` table until you delete them. Prune on a schedule with
`engine.prune(olderThanMs, limit?)`, which deletes done/failed/canceled runs older than the cutoff and
returns how many it removed (up to `limit`, default 1000):

```ts
// delete terminal runs older than 7 days, in batches
let removed: number;
do {
  removed = await engine.prune(7 * 24 * 60 * 60 * 1000, 1000);
} while (removed === 1000);
```

Keep completed runs short-lived. The tables are high-churn — a large backlog slows the claim query and
bloats the tables. On a busy instance, prune hourly.

### Autovacuum

Every claim and completion updates or deletes a `job` row, and Postgres leaves the old row versions for
autovacuum to reclaim. A **new** install creates the `job` table with an aggressive autovacuum setting —
vacuum at 2% dead rows, not the 20% default, which is too slow for a queue table.

`applySchema` sets this only when it creates the table, so your own tuning is never reset on a later
boot. Upgrading an existing database, or want it more aggressive under heavy load? Apply it yourself:

```sql
ALTER TABLE workflow.job SET (autovacuum_vacuum_scale_factor = 0.01);
```

Watch `n_dead_tup` for `job` in `pg_stat_user_tables`. If it climbs steadily, autovacuum is falling
behind. Don't turn autovacuum off on these tables — the dead rows never get reclaimed and the claim
query gets slower over time.

### Health checks

- `engine.health()` returns a count of runs per status. Use it for a quick overview; it's served at
  `GET /api/health` by the dashboard.
- `engine.liveness()` returns the dispatch backlog and the age of the oldest claimable run. Use it for
  a Kubernetes readiness probe.

### Autoscaling

`pending_work(flow_names, as_of)` returns the current backlog — claimable jobs plus due timers plus due
crons — as one number. Point KEDA's Postgres scaler at it to scale a worker pool up and down, including
to and from zero:

```sql
SELECT workflow.pending_work();               -- whole backlog
SELECT workflow.pending_work(ARRAY['greet']); -- one flow's shard
```

The dashboard also serves this at `GET /api/metrics` for a KEDA `metrics-api` scaler.

### Running a pool of workers

Workers judge lease expiry by their own clock, so keep clocks NTP-synced and set `leaseMs` above your
longest step plus the skew you expect. To split flows across workers, register different flows on each;
each claims only its own. See [deployment](../deployment.md).

## Optional features

### Faster wake-ups with LISTEN/NOTIFY

By default a worker polls for work. On a direct connection (not behind a transaction pooler), add
`LISTEN/NOTIFY` so a worker wakes the instant work is enqueued and a `result()` call wakes the instant
its run finishes.

```ts
import { createPgListener, applyNotifyTriggers } from "@iterativeflow/postgres";

await applyNotifyTriggers(sql); // install the triggers once; skip this behind a pooler
const listener = createPgListener(pool); // pass { schema } if you changed it — it must match
listener.start(); // opens the LISTEN connection; nothing fires until you call this

const backend = createPgBackend(sql, { listener });

// on shutdown, before pool.end():
await listener.close();
```

Don't turn this on behind a transaction pooler: `LISTEN` needs a dedicated connection, and a pooler
sends notifications to the wrong client. Polling still runs underneath, so a missed notification never
loses work — this only cuts latency.

### Retrying database errors in a step

Wrap a step that writes to Postgres with the `pgClassify` preset. Transient failures (deadlock,
serialization failure, connection drop) retry; permanent ones (not-null, check violation) fail the run
right away instead of retrying to no effect.

```ts
import { pgClassify } from "@iterativeflow/postgres";

run: async (ctx, input) => {
  await ctx.step("insert-order", () => db.insertOrder(input), { classify: pgClassify });
},
```

### Event timeline

To record a per-run event history for the dashboard, pass `createPgEventSink(sql)` as the engine's
`observe.sink`, and pass `listEvents` to `createDashboard`.

### Committing your own data with a run

`inTx(pool, fn)` runs `submit`/`enqueue` and your own writes in one transaction, so a run starts only
if your application write commits too. The callback receives a transaction-scoped backend and `Sql`:

```ts
import { inTx } from "@iterativeflow/postgres";

await inTx(pool, async (backend, tx) => {
  await tx.query("INSERT INTO orders (id, status) VALUES ($1, 'processing')", [orderId]);
  await submit(backend, fulfillOrder, { orderId });
});
```

## Troubleshooting

- **Wake-ups still poll after wiring the listener.** You didn't call `listener.start()`, or you're
  behind a transaction pooler, where `LISTEN` can't work.
- **Notifications stop after you change the schema.** The schema must match across `applySchema`,
  `createPgBackend`, `applyNotifyTriggers`, and `createPgListener`.
- **`prepared statement does not exist`, or session settings are ignored.** A transaction pooler is
  breaking session state. The engine itself is pooler-safe; this comes from other code on the same
  connection.
- **Claim latency climbs over time.** The `job` and `run` tables are bloated. Prune more aggressively
  and check autovacuum (above).

## Other backends

MySQL, SQLite, MongoDB, DynamoDB, Redis, and Durable Objects each have their own guide in this folder.
Cross-cutting topics (execution models, pooling, clocks, scaling, sharding) live in
[deployment](../deployment.md).
