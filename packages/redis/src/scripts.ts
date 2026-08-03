import { createHash } from "node:crypto";
import type { RedisClient } from "#client";

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
