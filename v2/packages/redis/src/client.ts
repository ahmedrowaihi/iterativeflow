import type { Redis } from "ioredis";

/** The ioredis connection the backend runs on. Pass a `new Redis(url)` (or a Cluster). */
export type RedisClient = Redis;
