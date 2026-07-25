---
"@iterativeflow/redis": minor
---

New backend: **`@iterativeflow/redis`** — the four ports over one ioredis connection, for Redis /
Valkey / Dragonfly (single node). State and dispatch share the keyspace, so the transactional outbox
commits as one Lua script; the Queue is sorted-set lease-CAS (crash recovery via lease expiry), the
Timer a `fireAt` sorted set, and Wakeup is in-process. Durability is the server's AOF config — the
fast, loss-tolerant tier. Passes the same store/queue/timer/wakeup/outbox/signal/reconcile/cron/engine
conformance suites as the Postgres and DynamoDB backends. `ioredis` is a peer dependency.
