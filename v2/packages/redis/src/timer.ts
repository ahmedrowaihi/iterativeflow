import type { Timer, TimerDueOpts } from "@iterativeflow/core/backend";
import type { RedisClient } from "#client";
import type { Keys } from "#keys";
import { ms } from "#time";

const DUE_BATCH = `
local due = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, ARGV[2])
if #due > 0 then
  redis.call('ZREM', KEYS[1], unpack(due))
end
return due
`;

/** @internal */
export const createRedisTimer = (client: RedisClient, keys: Keys): Timer => ({
  async schedule(runId, fireAt) {
    await client.zadd(keys.timers, fireAt.getTime(), runId);
  },

  async dueBatch({ now, limit }: TimerDueOpts) {
    return (await client.eval(DUE_BATCH, 1, keys.timers, ms(now), limit)) as string[];
  },

  async cancel(runId) {
    await client.zrem(keys.timers, runId);
  },
});
