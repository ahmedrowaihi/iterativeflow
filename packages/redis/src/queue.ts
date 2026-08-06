import type { ClaimOpts, IdGen, Lease, Queue } from "@iterativeflow/core/backend";
import { queueDepthOf } from "@iterativeflow/core/backend";
import type { RedisClient } from "#client";
import { JOB, type Keys, RUN } from "#keys";
import { luaRunner } from "#scripts";
import { ms } from "#time";

const CLAIM = `
local nowMs = tonumber(ARGV[1])
local leaseMs = tonumber(ARGV[2])
local limit = tonumber(ARGV[3])
local window = tonumber(ARGV[4])
local jobPre = ARGV[5]
local jobSuf = ARGV[6]
local runPre = ARGV[7]
local runSuf = ARGV[8]
local nameCount = tonumber(ARGV[9])
local wanted = nil
if nameCount >= 0 then
  wanted = {}
  for i = 1, nameCount do wanted[ARGV[9 + i]] = true end
end
local tokenBase = 9 + math.max(nameCount, 0)

local ids = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', nowMs, 'LIMIT', 0, window)
local due = {}
for _, runId in ipairs(ids) do
  local jobKey = jobPre .. runId .. jobSuf
  local f = redis.call('HMGET', jobKey, '${JOB.runAt}', '${JOB.priority}', '${JOB.version}', '${JOB.leaseExpires}')
  local runAt = tonumber(f[1])
  local leaseExpires = tonumber(f[4])
  if runAt ~= nil and runAt <= nowMs and (leaseExpires == nil or leaseExpires <= nowMs) then
    local keep = true
    if wanted ~= nil then
      local name = redis.call('HGET', runPre .. runId .. runSuf, '${RUN.name}')
      keep = name == false or wanted[name] == true
    end
    if keep then
      due[#due + 1] = { runId = runId, priority = tonumber(f[2]) or 0, runAt = runAt, version = tonumber(f[3]) or 0, key = jobKey }
    end
  end
end
table.sort(due, function(a, b)
  if a.priority ~= b.priority then return a.priority < b.priority end
  return a.runAt < b.runAt
end)
local out = {}
for i = 1, math.min(limit, #due) do
  local c = due[i]
  local token = ARGV[tokenBase + i] .. ':' .. c.runId
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
  const [jobPrefix, jobSuffix] = keys.jobAffix;
  const [runPrefix, runSuffix] = keys.runAffix;
  const run = luaRunner(client);

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

    async claim({ limit, leaseMs, now, names }: ClaimOpts) {
      const nowMs = ms(now);
      const tokens = Array.from({ length: limit }, () => id()); // Lua can't call IdGen; hand it fresh tokens
      const nameCount = names === undefined ? -1 : names.length;
      const rows = await run<[string, string, number][]>(
        CLAIM,
        [keys.queue],
        [
          nowMs,
          leaseMs,
          limit,
          limit * 10,
          jobPrefix,
          jobSuffix,
          runPrefix,
          runSuffix,
          nameCount,
          ...(names ?? []),
          ...tokens,
        ],
      );
      return rows.map((r): Lease => ({
        runId: r[0],
        token: r[1],
        expiresAt: new Date(nowMs + leaseMs),
        version: Number(r[2]),
      }));
    },

    async heartbeat(lease: Lease, { leaseMs, now }) {
      const nowMs = ms(now);
      const newExp = await run<number>(
        HEARTBEAT,
        [keys.job(lease.runId)],
        [nowMs, lease.token, leaseMs],
      );
      if (newExp < 0) throw new Error(`heartbeat: lease for ${lease.runId} is no longer held`);
      return { ...lease, expiresAt: new Date(newExp) };
    },

    async ack(lease: Lease, opts) {
      await run(
        ACK,
        [keys.job(lease.runId), keys.queue],
        [ms(opts?.now), lease.token, lease.version, lease.runId],
      );
    },

    async depth(now, names) {
      const nowMs = ms(now);
      const wanted = names && new Set(names);
      const runIds = await client.zrange(keys.queue, 0, -1);
      if (runIds.length === 0) return queueDepthOf([], nowMs);
      const pipe = client.pipeline();
      for (const runId of runIds) {
        pipe.hmget(keys.job(runId), JOB.runAt, JOB.leaseExpires);
        if (wanted) pipe.hget(keys.run(runId), RUN.name);
      }
      const res = (await pipe.exec()) ?? [];
      const stride = wanted ? 2 : 1;
      const jobs: { runAt: number; leaseExpires?: number }[] = [];
      for (let i = 0; i < runIds.length; i++) {
        if (wanted) {
          const name = res[i * stride + 1]?.[1] as string | null;
          if (name === null || !wanted.has(name)) continue;
        }
        const [runAt, leaseExpires] = (res[i * stride]?.[1] ?? []) as [
          string | null,
          string | null,
        ];
        if (runAt === null) continue;
        jobs.push({
          runAt: Number(runAt),
          leaseExpires: leaseExpires === null ? undefined : Number(leaseExpires),
        });
      }
      return queueDepthOf(jobs, nowMs);
    },
  };
};
