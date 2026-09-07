import type { Redis } from "ioredis";

/** The ioredis connection the backend runs on. Pass a `new Redis(url)` (single-node: the outbox Lua spans keys, so a Cluster fails with CROSSSLOT). */
export type RedisClient = Redis;
