import { type Backend, type IdGen, createLocalWakeup, newId } from "@iterativeflow/core/backend";
import type { RedisClient } from "#client";
import { makeKeys } from "#keys";
import { createRedisQueue } from "#queue";
import { createRedisStore } from "#store";
import { createRedisTimer } from "#timer";

export interface RedisBackendOpts {
  /** Key prefix namespacing every key this backend owns. Default `iterativeflow`. */
  prefix?: string;
  /** Id generator for runs, signals, and lease tokens. Defaults to {@link newId} (RFC-4122 v4). */
  id?: IdGen;
}

/**
 * The Redis {@link Backend}: the four ports over one ioredis connection. Store/Queue/Timer share the
 * keyspace, so an outbox commits as one Lua script — the single atomic domain the seam requires.
 * Wakeup is in-process ({@link createLocalWakeup}); a cross-process pub/sub wakeup is a future opt-in.
 * Durability is the server's persistence config (AOF) — this is the loss-tolerant tier.
 */
export const createRedisBackend = (client: RedisClient, opts: RedisBackendOpts = {}): Backend => {
  const keys = makeKeys(opts.prefix ?? "iterativeflow");
  const id = opts.id ?? newId;
  return {
    store: createRedisStore(client, keys, id),
    queue: createRedisQueue(client, keys, id),
    timer: createRedisTimer(client, keys),
    wakeup: createLocalWakeup(),
  };
};
