import type { Timer, TimerDueOpts } from "@iterativeflow/core/backend";
import type { RedisClient } from "#client";
import { type Keys, RUN } from "#keys";
import { luaRunner, replyList } from "#scripts";
import { ms } from "#time";

const DUE_BATCH = `
local due = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, ARGV[2])
if #due > 0 then
  redis.call('ZREM', KEYS[1], unpack(due))
end
return due
`;

/** @internal */
export const createRedisTimer = (client: RedisClient, keys: Keys): Timer => {
  const run = luaRunner(client);
  return {
    async schedule(runId, fireAt) {
      await client.zadd(keys.timers, fireAt.getTime(), runId);
    },

    async dueBatch({ now, limit }: TimerDueOpts) {
      return replyList(await run(DUE_BATCH, [keys.timers], [ms(now), limit]));
    },

    async cancel(runId) {
      await client.zrem(keys.timers, runId);
    },

    async nextDueAt(now) {
      // `(` makes the lower bound exclusive: a timer exactly at `now` is drained by the tick, not a horizon.
      const [, score] = await client.zrangebyscore(
        keys.timers,
        `(${ms(now)}`,
        "+inf",
        "WITHSCORES",
        "LIMIT",
        0,
        1,
      );
      return score === undefined ? null : new Date(Number(score));
    },

    async dueCount(now, names) {
      if (names?.length === 0) return 0;
      const wanted = names && new Set(names);
      if (!wanted) return client.zcount(keys.timers, "-inf", ms(now));
      const due = await client.zrangebyscore(keys.timers, "-inf", ms(now));
      if (due.length === 0) return 0;
      const runNames = await Promise.all(
        due.map((runId) => client.hget(keys.run(runId), RUN.name)),
      );
      // a run-less timer is unownable, so it passes every name filter (see Queue.claim)
      return runNames.filter((name) => name === null || wanted.has(name)).length;
    },
  };
};
