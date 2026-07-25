# @iterativeflow/redis

A Redis-family backend for [iterativeflow](https://github.com/ahmedrowaihi/iterativeflow) — the four
ports (Store / Queue / Timer / Wakeup) over one [ioredis](https://github.com/redis/ioredis)
connection. Runs on **Redis, Valkey, or Dragonfly** (single node).

State and dispatch both live in Redis, so the transactional outbox commits as **one Lua script** —
the single atomic domain the engine needs. Durability is your server's persistence config (AOF); this
is the fast, loss-tolerant tier. It passes the same conformance suites as the Postgres and DynamoDB
backends.

```ts
import { Redis } from "ioredis";
import { createRedisBackend } from "@iterativeflow/redis";
import { createEngine, defineFlow } from "@iterativeflow/core";

const backend = createRedisBackend(new Redis(process.env.REDIS_URL));
const engine = createEngine(backend, [
  /* your flows */
]);
```

## Notes

- **`ioredis` is a peer dependency** — bring your own client (or a Cluster).
- **Wakeup is in-process** (like the DynamoDB backend). A run completing in another process is caught
  by the poll-first `result()`; a cross-process pub/sub wakeup is a future opt-in.
- **Retention**: runs are pruned via `engine.prune`; you can also set an `AOF`/eviction policy to age
  out terminal runs. Configure AOF (`appendfsync everysec` or `always`) to set your durability window.
- **Cluster**: the per-run keys are hash-tagged, but the outbox Lua also touches the shared dispatch
  keys, so this targets a single node for now. See `docs/v2/redis-backend-study.md`.
