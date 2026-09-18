import { createHash } from "node:crypto";
import type { RedisClient } from "#client";
import { JOB, RUN } from "#keys";

/**
 * The enqueue contract shared by the Queue and the Store's outbox: ZADD the queue by runAt (ms), then
 * stamp the job hash and bump its version. A nil/'' priority takes the run's stored one, else 0.
 * @internal
 */
export const ENQUEUE_FN = `
local function iflow_enqueue(qKey, runId, jobKey, runKey, runAtMs, priority)
  if priority == nil or priority == '' then
    priority = redis.call('HGET', runKey, '${RUN.priority}') or 0
  end
  redis.call('ZADD', qKey, runAtMs, runId)
  redis.call('HSET', jobKey, '${JOB.runAt}', runAtMs, '${JOB.priority}', priority)
  redis.call('HINCRBY', jobKey, '${JOB.version}', 1)
end`;

type Command = (...a: (string | number)[]) => Promise<unknown>;
type Run = (keys: string[], args: (string | number)[]) => Promise<unknown>;

/**
 * A content-addressed Lua runner. Each distinct script is registered once as an EVALSHA-cached custom
 * command named by its hash — ioredis then ships the body only on the first call (or a `NOSCRIPT`
 * miss), not on every invocation, so the hot outbox/claim scripts aren't re-sent per step. Hashing
 * the body makes the command name stable across store/queue instances that share a client.
 */
export const luaRunner = (client: RedisClient) => {
  const runners = new Map<string, Run>();
  const c = client as unknown as Record<string, Command | undefined>;
  return <T>(lua: string, keys: string[], args: (string | number)[]): Promise<T> => {
    let run = runners.get(lua);
    if (!run) {
      const cmd = `iflow_${createHash("sha1").update(lua).digest("hex").slice(0, 16)}`;
      if (!c[cmd]) client.defineCommand(cmd, { lua });
      run = (k, a) => c[cmd]!(k.length, ...k, ...a);
      runners.set(lua, run);
    }
    return run(keys, args) as Promise<T>;
  };
};
