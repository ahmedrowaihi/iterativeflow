import type { Timer, TimerDueOpts } from "@iterativeflow/core/backend";
import type { RedisClient } from "#client";
import { type Keys, RUN } from "#keys";
import { luaRunner } from "#scripts";
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
      return run<string[]>(DUE_BATCH, [keys.timers], [ms(now), limit]);
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
      const wanted = names && new Set(names);
      if (!wanted) return client.zcount(keys.timers, "-inf", ms(now));
      const due = await client.zrangebyscore(keys.timers, "-inf", ms(now));
      if (due.length === 0) return 0;
      const pipe = client.pipeline();
      for (const runId of due) pipe.hget(keys.run(runId), RUN.name);
      const res = (await pipe.exec()) ?? [];
      let n = 0;
      for (let i = 0; i < due.length; i++) {
        const name = res[i]?.[1] as string | null;
        if (name !== null && wanted.has(name)) n += 1;
      }
      return n;
    },
  };
};
