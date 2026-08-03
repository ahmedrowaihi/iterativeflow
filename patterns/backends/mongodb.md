# MongoDB

`@iterativeflow/mongodb` runs the engine on MongoDB. Use it when MongoDB is already your database.

MongoDB needs a **replica set**. The engine commits an outbox as a multi-document transaction, and
MongoDB transactions don't work on a standalone `mongod`. A single-node replica set is enough for local
development.

## Requirements

- A replica set (or sharded cluster). Standalone `mongod` will fail transactions.
- A `mongodb` `MongoClient`. The library never opens connections on its own.
- Write concern `majority` (the default on MongoDB 5.0+). With a lower write concern, an acknowledged
  write can be rolled back on failover — silent data loss for a system of record. Don't override it
  below `majority`.

## Install

```bash
npm install @iterativeflow/mongodb @iterativeflow/core mongodb
```

`mongodb` is a peer dependency (6.0 or newer).

## Set up the database

`ensureIndexes` creates the indexes the engine needs (idempotent). It doesn't create collections —
MongoDB makes those on first write.

```ts
// db.ts
import { MongoClient } from "mongodb";
import { ensureIndexes } from "@iterativeflow/mongodb";

export const client = new MongoClient(process.env.MONGO_URL!, { writeConcern: { w: "majority" } });
await client.connect();

await ensureIndexes(client.db("iterativeflow"));
```

To run several engines in one database, pass a prefix: `ensureIndexes(client.db("iterativeflow"), "flows_")`
and `createMongoBackend(client, { prefix: "flows_" })`. Pass a different `db` name via
`createMongoBackend(client, { db: "myapp" })` — it must match the `client.db(...)` you index.

## Run a worker

```ts
// worker.ts
import { createMongoBackend } from "@iterativeflow/mongodb";
import { createEngine, defineFlow } from "@iterativeflow/core";
import { client } from "./db";

const backend = createMongoBackend(client, { db: "iterativeflow" });

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
await client.close();
```

For serverless, call `serverlessTick(backend, registry([greet]), { batchMax: 20, leaseMs: 30_000 })`
per invocation instead of `engine.run()`.

## Operating in production

### Read from the primary

Keep the read preference at `primary` (the default). Secondaries lag, so a worker reading a claim from a
secondary can see stale state and re-run completed work. Transactions read from the primary anyway.

### Write concern

Leave write concern at `majority`. A run's state is the system of record; `w:1` can lose acknowledged
writes on failover. Prefer a primary-secondary-secondary topology over one with an arbiter.

### Retention

Prune terminal runs on a schedule: `await engine.prune(7 * 24 * 60 * 60 * 1000)`. You can also use a
MongoDB TTL index for coarse cleanup, but TTL deletion is a background sweep (up to a minute late, and
only the primary deletes), so don't treat it as prompt or as a correctness boundary.

### Health checks and autoscaling

- `engine.health()` returns run counts per status; `engine.liveness()` returns the dispatch backlog and
  oldest-claimable age.
- For autoscaling, serve `engine.pendingWork(names?)` over HTTP (the dashboard's `GET /api/metrics`) for
  a KEDA `metrics-api` scaler.

### Running a pool of workers

Workers judge lease expiry by their own clock — keep clocks NTP-synced and size `leaseMs` above your
longest step plus skew. See [deployment](../deployment.md).

## Limits

- **16 MB per document.** A run's input, output, and step results are stored on its documents; very
  large payloads can hit the limit and the write fails. Keep big blobs in object storage and store a
  reference.

## Optional features

`inTx(client, (backend, session) => ...)` runs `submit`/`enqueue` and your own writes in one
transaction, so a run starts only if your application write commits too. The callback receives a
transaction-scoped backend and the MongoDB `ClientSession` to use for your own collection writes.

## Troubleshooting

- **`Transaction numbers are only allowed on a replica set member or mongos`.** You're on a standalone
  `mongod`. Start a replica set (a single-node set is fine for dev).
- **A worker re-runs completed jobs.** Something is reading from a secondary — set the read preference
  back to `primary`.
- **An acknowledged run vanished after a failover.** Write concern was below `majority`.

## Other backends

Postgres, MySQL, SQLite, DynamoDB, Redis, and Durable Objects each have their own guide in this folder.
Cross-cutting topics live in [deployment](../deployment.md).
