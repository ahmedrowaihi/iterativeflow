# Redis

`@iterativeflow/redis` runs the engine on Redis. Use it when Redis is already in your stack and you
want low-latency claims.

> **Configure Redis for durability first.** Redis's default configuration is tuned for caching, and a
> cache-configured Redis silently loses a durable engine's data. Read the next section before running
> this in production.

## Durability configuration (read this first)

The engine's state is a system of record, not a cache. Two Redis defaults will destroy it silently —
successful writes vanish with no error:

- **Set `maxmemory-policy` to `noeviction`.** Any `allkeys-lru` / `volatile-lru` / `*-lfu` / `*-random`
  policy evicts live run, job, timer, and lease keys when memory fills — with no error. `noeviction`
  makes writes fail loudly (an OOM error) instead of dropping data. This is the single most important
  setting.
- **Enable AOF persistence.** With RDB snapshots only (the default), a crash or restart loses every
  write since the last snapshot — minutes of acknowledged work. Set `appendonly yes`. `appendfsync
everysec` (the AOF default) still loses up to ~1s of acknowledged writes on a host crash; use
  `appendfsync always` if you can't afford that (at a throughput cost).
- **Replication:** an acknowledged write on a single node can be lost if the node fails before it
  replicates. For the strongest durability, run replicas and require replica acks.

Managed variants: **Upstash** persists to durable storage and disables eviction by default (good
defaults — leave eviction off). **ElastiCache**: you must set `maxmemory-policy noeviction` and AOF
yourself.

## Requirements

- An `ioredis` client over a **TCP socket**. A REST Redis (Upstash's REST API) is not supported — the
  engine uses pipelines and Lua scripts that need a socket connection.
- Redis configured for durability (above).

## Install

```bash
npm install @iterativeflow/redis @iterativeflow/core ioredis
```

`ioredis` is a peer dependency (5.0 or newer).

## Run a worker

Redis needs no schema step — there are no tables to create.

```ts
// worker.ts
import Redis from "ioredis";
import { createRedisBackend } from "@iterativeflow/redis";
import { createEngine, defineFlow } from "@iterativeflow/core";

const client = new Redis(process.env.REDIS_URL!);
const backend = createRedisBackend(client);

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
await client.quit();
```

To run several engines on one Redis, pass a key prefix: `createRedisBackend(client, { prefix: "flows" })`.
For serverless, call `serverlessTick(backend, registry([greet]), { batchMax: 20, leaseMs: 30_000 })` per
invocation.

## Operating in production

### Redis Cluster is not supported yet

Run on a single node, optionally with replicas — not Redis Cluster. The engine's claim uses Lua scripts
that touch both a run's keys and shared dispatch keys (the queue and timer indexes). The shared keys
aren't hash-tagged, so on a cluster the script would span hash slots and fail with `CROSSSLOT`. Per-run
keys already carry a `{runId}` tag as groundwork for future cluster support, but it isn't complete.
Single-node Redis, Valkey, or Dragonfly are fine.

### Wake-ups

Wake-ups are in-process: a worker wakes its own waiters immediately, and other worker processes pick up
work on their next poll. There's no cross-process push, so with many worker processes, tune the poll
interval for the latency you want.

### Retention

Prune terminal runs on a schedule: `await engine.prune(7 * 24 * 60 * 60 * 1000)`. This bounds memory —
important on Redis, where everything lives in RAM.

### Health checks and autoscaling

- `engine.health()` returns run counts per status; `engine.liveness()` returns the dispatch backlog and
  oldest-claimable age.
- For autoscaling, serve `engine.pendingWork(names?)` over HTTP (the dashboard's `GET /api/metrics`) for
  a KEDA `metrics-api` scaler.

### Running a pool of workers

Workers judge lease expiry by their own clock — keep clocks NTP-synced and size `leaseMs` above your
longest step plus skew. See [deployment](../deployment.md).

## Troubleshooting

- **Runs disappear with no error.** Eviction is on. Set `maxmemory-policy noeviction`.
- **Runs lost after a Redis restart.** Persistence is off or RDB-only. Enable AOF.
- **`CROSSSLOT` errors.** You're on Redis Cluster, which isn't supported yet — use a single node.
- **Connection errors with a REST Redis.** The REST API isn't supported — use a TCP `ioredis` client.

## Other backends

Postgres, MySQL, SQLite, MongoDB, DynamoDB, and Durable Objects each have their own guide in this
folder. Cross-cutting topics live in [deployment](../deployment.md).
