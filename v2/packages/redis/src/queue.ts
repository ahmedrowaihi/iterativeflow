import type { ClaimOpts, IdGen, Lease, Queue } from "@iterativeflow/core/backend";
import { queueDepthOf } from "@iterativeflow/core/backend";
import type { RedisClient } from "#client";
import { JOB, type Keys } from "#keys";

const ms = (now?: Date): number => (now ?? new Date()).getTime();

const CLAIM = `
local nowMs = tonumber(ARGV[1])
local leaseMs = tonumber(ARGV[2])
local limit = tonumber(ARGV[3])
local window = tonumber(ARGV[4])
local jobPre = ARGV[5]
local jobSuf = ARGV[6]

local ids = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', nowMs, 'LIMIT', 0, window)
local due = {}
for _, runId in ipairs(ids) do
  local jobKey = jobPre .. runId .. jobSuf
  local f = redis.call('HMGET', jobKey, '${JOB.runAt}', '${JOB.priority}', '${JOB.version}', '${JOB.leaseExpires}')
  local runAt = tonumber(f[1])
  local leaseExpires = tonumber(f[4])
  if runAt ~= nil and runAt <= nowMs and (leaseExpires == nil or leaseExpires <= nowMs) then
    due[#due + 1] = { runId = runId, priority = tonumber(f[2]) or 0, runAt = runAt, version = tonumber(f[3]) or 0, key = jobKey }
  end
end
table.sort(due, function(a, b)
  if a.priority ~= b.priority then return a.priority < b.priority end
  return a.runAt < b.runAt
end)
local out = {}
for i = 1, math.min(limit, #due) do
  local c = due[i]
  local token = ARGV[6 + i] .. ':' .. c.runId
  redis.call('HSET', c.key, '${JOB.leaseToken}', token, '${JOB.leaseExpires}', nowMs + leaseMs)
  out[i] = { c.runId, token, c.version }
end
return out
`;

const HEARTBEAT = `
local nowMs = tonumber(ARGV[1])
local token = ARGV[2]
local exp = tonumber(redis.call('HGET', KEYS[1], '${JOB.leaseExpires}'))
if redis.call('HGET', KEYS[1], '${JOB.leaseToken}') == token and exp ~= nil and exp > nowMs then
  local newExp = nowMs + tonumber(ARGV[3])
  redis.call('HSET', KEYS[1], '${JOB.leaseExpires}', newExp)
  return newExp
end
return -1
`;

const ACK = `
local nowMs = tonumber(ARGV[1])
local token = ARGV[2]
local version = tonumber(ARGV[3])
local runId = ARGV[4]
local exp = tonumber(redis.call('HGET', KEYS[1], '${JOB.leaseExpires}'))
if redis.call('HGET', KEYS[1], '${JOB.leaseToken}') ~= token or exp == nil or exp <= nowMs then
  return 0
end
if tonumber(redis.call('HGET', KEYS[1], '${JOB.version}')) == version then
  redis.call('ZREM', KEYS[2], runId)
  redis.call('DEL', KEYS[1])
else
  redis.call('HDEL', KEYS[1], '${JOB.leaseToken}', '${JOB.leaseExpires}')
  redis.call('HSET', KEYS[1], '${JOB.runAt}', 0)
  redis.call('ZADD', KEYS[2], 0, runId)
end
return 1
`;

/** @internal */
export const createRedisQueue = (client: RedisClient, keys: Keys, id: IdGen): Queue => {
  // CLAIM builds each job key inside Lua (it can't call keys.job), so hand it the affixes around runId.
  const [jobPrefix, jobSuffix] = keys.job("\u0000").split("\u0000");

  return {
    async enqueue(runId, opts) {
      const runAtMs = opts?.runAt ? opts.runAt.getTime() : 0;
      const priority = opts?.priority ?? 0;
      await client
        .multi()
        .zadd(keys.queue, runAtMs, runId)
        .hset(keys.job(runId), JOB.runAt, runAtMs, JOB.priority, priority)
        .hincrby(keys.job(runId), JOB.version, 1)
        .exec();
    },

    async claim({ limit, leaseMs, now }: ClaimOpts) {
      const nowMs = ms(now);
      const tokens = Array.from({ length: limit }, () => id()); // Lua can't call IdGen; hand it fresh tokens
      const rows = (await client.eval(
        CLAIM,
        1,
        keys.queue,
        nowMs,
        leaseMs,
        limit,
        limit * 10,
        jobPrefix,
        jobSuffix,
        ...tokens,
      )) as [string, string, number][];
      return rows.map(
        (r): Lease => ({
          runId: r[0],
          token: r[1],
          expiresAt: new Date(nowMs + leaseMs),
          version: Number(r[2]),
        }),
      );
    },

    async heartbeat(lease: Lease, { leaseMs, now }) {
      const nowMs = ms(now);
      const newExp = (await client.eval(
        HEARTBEAT,
        1,
        keys.job(lease.runId),
        nowMs,
        lease.token,
        leaseMs,
      )) as number;
      if (newExp < 0) throw new Error(`heartbeat: lease for ${lease.runId} is no longer held`);
      return { ...lease, expiresAt: new Date(newExp) };
    },

    async ack(lease: Lease, opts) {
      await client.eval(
        ACK,
        2,
        keys.job(lease.runId),
        keys.queue,
        ms(opts?.now),
        lease.token,
        lease.version,
        lease.runId,
      );
    },

    async depth(now) {
      const nowMs = ms(now);
      const runIds = await client.zrange(keys.queue, 0, -1);
      if (runIds.length === 0) return queueDepthOf([], nowMs);
      const pipe = client.pipeline();
      for (const runId of runIds) pipe.hmget(keys.job(runId), JOB.runAt, JOB.leaseExpires);
      const res = await pipe.exec();
      const jobs = (res ?? []).map(([, fields]) => {
        const [runAt, leaseExpires] = fields as [string | null, string | null];
        return {
          runAt: Number(runAt),
          leaseExpires: leaseExpires === null ? undefined : Number(leaseExpires),
        };
      });
      return queueDepthOf(jobs, nowMs);
    },
  };
};
