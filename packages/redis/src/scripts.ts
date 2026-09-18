import { createHash } from "node:crypto";
import type { RedisClient } from "#client";
import { JOB, RUN } from "#keys";

/** @internal */
export const ENQUEUE_FN = `
local function iflow_enqueue(qKey, runId, jobKey, runKey, runAtMs, priority)
  if priority == nil or priority == '' then
    priority = redis.call('HGET', runKey, '${RUN.priority}') or 0
  end
  redis.call('ZADD', qKey, runAtMs, runId)
  redis.call('HSET', jobKey, '${JOB.runAt}', runAtMs, '${JOB.priority}', priority)
  redis.call('HINCRBY', jobKey, '${JOB.version}', 1)
end`;

/**
 * A script reply with every scalar in its string form (an integer reply arrives as `"1"`, Lua
 * `nil`/`false` as `null`). The scripts here nest at most two levels (CLAIM's rows).
 * @internal
 */
export type LuaReply = string | null | (string | null | string[])[];

const unexpected = (reply: LuaReply, want: string): Error =>
  new Error(`redis: expected a Lua ${want} reply, got ${JSON.stringify(reply)}`);

/** @internal */
export const replyText = (reply: LuaReply): string => {
  if (reply === null || Array.isArray(reply)) throw unexpected(reply, "string");
  return reply;
};

/** @internal */
export const replyNumber = (reply: LuaReply): number | undefined => {
  if (reply === null) return undefined;
  const n = Number(replyText(reply));
  if (Number.isNaN(n)) throw unexpected(reply, "number");
  return n;
};

/** @internal */
export const replyList = (reply: LuaReply): string[] => {
  if (!Array.isArray(reply)) throw unexpected(reply, "list");
  return reply.map((item) => {
    if (item === null || Array.isArray(item)) throw unexpected(reply, "list of strings");
    return item;
  });
};

/** @internal */
export const replyRows = (reply: LuaReply): string[][] => {
  if (!Array.isArray(reply)) throw unexpected(reply, "list");
  return reply.map((item) => {
    if (!Array.isArray(item)) throw unexpected(reply, "list of rows");
    return item;
  });
};

/**
 * A content-addressed Lua runner: EVALSHA first, shipping the body only on a `NOSCRIPT` miss, so the
 * hot outbox/claim scripts aren't re-sent per step. The reply is decoded into a {@link LuaReply}.
 */
export const luaRunner = (client: RedisClient) => {
  const shas = new Map<string, string>();
  return async (lua: string, keys: string[], args: (string | number)[]): Promise<LuaReply> => {
    let sha = shas.get(lua);
    if (!sha) {
      sha = createHash("sha1").update(lua).digest("hex");
      shas.set(lua, sha);
    }
    const raw = await client.evalsha(sha, keys.length, ...keys, ...args).catch((cause) => {
      if (!(cause instanceof Error && cause.message.startsWith("NOSCRIPT"))) throw cause;
      return client.eval(lua, keys.length, ...keys, ...args);
    });
    if (raw === null || raw === undefined) return null;
    if (!Array.isArray(raw)) return String(raw);
    return raw.map((item) =>
      item === null ? null : Array.isArray(item) ? item.map(String) : String(item),
    );
  };
};
