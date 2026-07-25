import {
  type CronSpec,
  type IdGen,
  type Outbox,
  type RunFilter,
  type RunRow,
  type RunSpec,
  type RunStatus,
  type StartResult,
  type StepCheckpoint,
  type StepOutcome,
  type Store,
  type SuspendStatus,
  type TerminalOutcome,
  type OrphanView,
  type Page,
  TERMINAL_STATUSES,
  isOrphaned,
  isTerminal,
  statusList,
  zeroRunStats,
} from "@iterativeflow/core/backend";
import type { RedisClient } from "#client";
import { JOB, type Keys, RUN } from "#keys";
import {
  cronRowFromSpec,
  decodeCron,
  decodeSignal,
  decodeStep,
  encodeCron,
  encodeStep,
  idemIdentity,
  runFields,
  toRunRow,
} from "#codec";

type Hash = Record<string, string>;

// Field names come from the pinned RUN/JOB maps; only status VALUES (`running`, `pending`, …) are
// literals here, exactly as in the memory oracle.
const IS_TERMINAL = TERMINAL_STATUSES.map((s) => `s == '${s}'`).join(" or ");

const TERMINAL_FN = `
local function iflow_terminal(s)
  return ${IS_TERMINAL}
end`;

// Enqueue contract shared verbatim with the standalone Queue: ZADD the queue by runAt (ms), then
// stamp the job hash and bump its version.
const OUTBOX_LIB = `
local function iflow_enqueue(qKey, runId, jobKey, runAtMs, priority)
  redis.call('ZADD', qKey, runAtMs, runId)
  redis.call('HSET', jobKey, '${JOB.runAt}', runAtMs, '${JOB.priority}', priority)
  redis.call('HINCRBY', jobKey, '${JOB.version}', 1)
end

local function iflow_apply(fx, qKey, idxKey, tmrKey, seqKey, idemKey, inboxKey)
  if fx.spawn then
    for _, s in ipairs(fx.spawn) do
      local seq = redis.call('INCR', seqKey)
      redis.call('HSET', s.runKey, unpack(s.fields))
      redis.call('HSET', s.runKey, '${RUN.seq}', seq)
      redis.call('ZADD', idxKey, seq, s.childId)
      if s.childrenKey then redis.call('SADD', s.childrenKey, s.childId) end
      if s.idemField then redis.call('HSET', idemKey, s.idemField, s.childId) end
      iflow_enqueue(qKey, s.childId, s.jobKey, s.runAtMs, s.priority)
    end
  end
  if fx.joinTarget then
    redis.call('HSET', fx.joinTarget.runKey, '${RUN.joinRemaining}', fx.joinTarget.count)
  end
  if fx.enqueue then
    for _, e in ipairs(fx.enqueue) do
      iflow_enqueue(qKey, e.runId, e.jobKey, e.runAtMs, e.priority)
    end
  end
  if fx.timers then
    for _, t in ipairs(fx.timers) do
      redis.call('ZADD', tmrKey, t.fireAtMs, t.runId)
    end
  end
  if fx.cancelTimers then
    for _, r in ipairs(fx.cancelTimers) do
      redis.call('ZREM', tmrKey, r)
    end
  end
  if fx.consumeSignals then
    local items = redis.call('LRANGE', inboxKey, 0, -1)
    for _, sid in ipairs(fx.consumeSignals) do
      for _, raw in ipairs(items) do
        if cjson.decode(raw).id == sid then
          redis.call('LREM', inboxKey, 1, raw)
          break
        end
      end
    end
  end
end`;

const START_LUA = `
local idemField, fieldsJson, runId, childrenKey = ARGV[1], ARGV[2], ARGV[3], ARGV[4]
if idemField ~= '' then
  local existing = redis.call('HGET', KEYS[4], idemField)
  if existing then return {'hit', existing} end
end
local seq = redis.call('INCR', KEYS[3])
redis.call('HSET', KEYS[1], unpack(cjson.decode(fieldsJson)))
redis.call('HSET', KEYS[1], '${RUN.seq}', seq)
redis.call('ZADD', KEYS[2], seq, runId)
if idemField ~= '' then redis.call('HSET', KEYS[4], idemField, runId) end
if childrenKey ~= '' then redis.call('SADD', childrenKey, runId) end
return {'new'}`;

const POST_SIGNAL_LUA = `${OUTBOX_LIB}
if ARGV[1] ~= '' then
  if redis.call('SADD', KEYS[1], ARGV[1]) == 0 then return 0 end
end
redis.call('RPUSH', KEYS[2], ARGV[2])
iflow_enqueue(KEYS[4], ARGV[3], KEYS[3], ARGV[4], ARGV[5])
return 1`;

const MARK_RUNNING_LUA = `${TERMINAL_FN}
local s = redis.call('HGET', KEYS[1], '${RUN.status}')
if s == false then return end
if iflow_terminal(s) then return tonumber(redis.call('HGET', KEYS[1], '${RUN.attempts}')) end
redis.call('HSET', KEYS[1], '${RUN.status}', 'running')
return redis.call('HINCRBY', KEYS[1], '${RUN.attempts}', 1)`;

const ARRIVE_LUA = `
if redis.call('EXISTS', KEYS[1]) == 0 then return end
return redis.call('HINCRBY', KEYS[1], '${RUN.joinRemaining}', -1)`;

const CHECKPOINT_LUA = `${OUTBOX_LIB}
if redis.call('EXISTS', KEYS[1]) == 0 then return {'noRun'} end
if redis.call('HSETNX', KEYS[2], ARGV[1], ARGV[2]) == 0 then
  return {'hit', redis.call('HGET', KEYS[2], ARGV[1])}
end
if ARGV[3] ~= '' then
  iflow_apply(cjson.decode(ARGV[3]), KEYS[4], KEYS[5], KEYS[6], KEYS[7], KEYS[8], KEYS[3])
end
return {'ok'}`;

const SUSPEND_LUA = `${TERMINAL_FN}${OUTBOX_LIB}
local s = redis.call('HGET', KEYS[1], '${RUN.status}')
if s == false then return 'noRun' end
if iflow_terminal(s) then return 'ok' end
redis.call('HSET', KEYS[1], '${RUN.status}', ARGV[1])
if ARGV[2] == '1' then redis.call('HSET', KEYS[1], '${RUN.attempts}', 0) end
if ARGV[3] ~= '' then
  iflow_apply(cjson.decode(ARGV[3]), KEYS[3], KEYS[4], KEYS[5], KEYS[6], KEYS[7], KEYS[2])
end
return 'ok'`;

const MARK_TERMINAL_LUA = `${OUTBOX_LIB}
local s = redis.call('HGET', KEYS[1], '${RUN.status}')
if s == false then return 'noRun' end
if s == 'canceled' then return 'ok' end
redis.call('HSET', KEYS[1], '${RUN.status}', ARGV[1])
if ARGV[2] == '1' then redis.call('HSET', KEYS[1], '${RUN.output}', ARGV[3])
else redis.call('HDEL', KEYS[1], '${RUN.output}') end
if ARGV[4] == '1' then redis.call('HSET', KEYS[1], '${RUN.error}', ARGV[5])
else redis.call('HDEL', KEYS[1], '${RUN.error}') end
if ARGV[6] ~= '' then
  iflow_apply(cjson.decode(ARGV[6]), KEYS[3], KEYS[4], KEYS[5], KEYS[6], KEYS[7], KEYS[2])
end
return 'ok'`;

const RETRY_LUA = `${OUTBOX_LIB}
local s = redis.call('HGET', KEYS[1], '${RUN.status}')
if s == false then return -1 end
if s ~= 'failed' then return 0 end
redis.call('HSET', KEYS[1], '${RUN.status}', 'pending')
redis.call('HDEL', KEYS[1], '${RUN.error}')
iflow_enqueue(KEYS[3], ARGV[1], KEYS[2], ARGV[2], ARGV[3])
return 1`;

const UPSERT_CRON_LUA = `
local existing = redis.call('HGET', KEYS[1], ARGV[1])
local row, score = ARGV[2], ARGV[3]
if existing ~= false then
  local prev = cjson.decode(existing)
  local obj = cjson.decode(ARGV[2])
  obj.nextRunAt = prev.nextRunAt
  obj.lastRunAt = prev.lastRunAt
  row = cjson.encode(obj)
  local zs = redis.call('ZSCORE', KEYS[2], ARGV[1])
  if zs ~= false then score = zs end
end
redis.call('HSET', KEYS[1], ARGV[1], row)
redis.call('ZADD', KEYS[2], score, ARGV[1])`;

const ADVANCE_CRON_LUA = `
local cur = redis.call('ZSCORE', KEYS[2], ARGV[1])
if cur == false or tonumber(cur) ~= tonumber(ARGV[2]) then return 0 end
local raw = redis.call('HGET', KEYS[1], ARGV[1])
if raw == false then return 0 end
local obj = cjson.decode(raw)
obj.nextRunAt = ARGV[4]
obj.lastRunAt = ARGV[5]
redis.call('HSET', KEYS[1], ARGV[1], cjson.encode(obj))
redis.call('ZADD', KEYS[2], ARGV[3], ARGV[1])
return 1`;

/** The Redis {@link Store}: durable run state + the transactional outbox over one ioredis client. */
export const createRedisStore = (client: RedisClient, keys: Keys, id: IdGen): Store => {
  const evalLua = <T>(script: string, k: string[], args: (string | number)[]): Promise<T> =>
    client.eval(script, k.length, ...k, ...args) as Promise<T>;

  const flatFields = (spec: RunSpec, runId: string): string[] => {
    const f = runFields(spec, runId);
    const out: string[] = [];
    for (const [k, v] of Object.entries(f)) out.push(k, v);
    return out;
  };

  // Prebuild every dynamic key + child hash in JS, so the Lua only applies what it's handed — keeping
  // the atomic domain single-node (per-run keys are hash-tagged but the shared keys are not).
  const serializeOutbox = (fx?: Outbox): string => {
    if (!fx) return "";
    // runAt default 0 (epoch = immediately claimable) — must match Queue.enqueue and the memory oracle.
    const enq = (runId: string, opts?: { runAt?: Date; priority?: number }) => ({
      runId,
      jobKey: keys.job(runId),
      runAtMs: opts?.runAt ? opts.runAt.getTime() : 0,
      priority: opts?.priority ?? 0,
    });
    const blob: Record<string, unknown> = {};
    if (fx.spawn?.length) {
      blob.spawn = fx.spawn.map((s) => {
        const e = enq(s.runId, s.enqueue);
        return {
          runKey: keys.run(s.runId),
          childId: s.runId,
          fields: flatFields(s.spec, s.runId),
          childrenKey:
            s.spec.parentRunId !== undefined ? keys.children(s.spec.parentRunId) : undefined,
          idemField:
            s.spec.idempotencyKey !== undefined
              ? idemIdentity(s.spec.name, s.spec.version, s.spec.idempotencyKey)
              : undefined,
          jobKey: e.jobKey,
          runAtMs: e.runAtMs,
          priority: e.priority,
        };
      });
    }
    if (fx.joinTarget) {
      blob.joinTarget = { runKey: keys.run(fx.joinTarget.runId), count: fx.joinTarget.count };
    }
    if (fx.enqueue?.length) blob.enqueue = fx.enqueue.map((x) => enq(x.runId, x.opts));
    if (fx.timers?.length) {
      blob.timers = fx.timers.map((t) => ({ runId: t.runId, fireAtMs: t.fireAt.getTime() }));
    }
    if (fx.cancelTimers?.length) blob.cancelTimers = [...fx.cancelTimers];
    if (fx.consumeSignals?.length) blob.consumeSignals = [...fx.consumeSignals];
    return JSON.stringify(blob);
  };

  const loadRunRow = async (runId: string): Promise<RunRow | undefined> =>
    toRunRow((await client.hgetall(keys.run(runId))) as Hash);

  const loadRunRows = async (runIds: readonly string[]): Promise<(RunRow | undefined)[]> => {
    if (runIds.length === 0) return [];
    const pipe = client.pipeline();
    for (const runId of runIds) pipe.hgetall(keys.run(runId));
    const res = await pipe.exec();
    return runIds.map((_, i) => toRunRow((res?.[i]?.[1] ?? {}) as Hash));
  };

  const startOne = async (spec: RunSpec): Promise<StartResult> => {
    const runId = id();
    const idemField =
      spec.idempotencyKey !== undefined
        ? idemIdentity(spec.name, spec.version, spec.idempotencyKey)
        : "";
    const childrenKey = spec.parentRunId !== undefined ? keys.children(spec.parentRunId) : "";
    const res = await evalLua<[string, string?]>(
      START_LUA,
      [keys.run(runId), keys.runIndex, keys.seq, keys.idem],
      [idemField, JSON.stringify(flatFields(spec, runId)), runId, childrenKey],
    );
    if (res[0] === "new") return { runId, created: true, status: "pending" };
    const existingId = res[1] as string;
    const row = await loadRunRow(existingId);
    if (!row) throw new Error("startRun: idempotency index points at a missing run");
    return { runId: existingId, created: false, status: row.status };
  };

  return {
    startRun: startOne,

    async startManyRuns(specs) {
      return Promise.all(specs.map(startOne));
    },

    async loadRun(runId) {
      const [[, rawRun], [, rawSteps], [, rawSignals]] = (await client
        .pipeline()
        .hgetall(keys.run(runId))
        .hgetall(keys.steps(runId))
        .lrange(keys.inbox(runId), 0, -1)
        .exec()) as [[Error | null, Hash], [Error | null, Hash], [Error | null, string[]]];
      const run = toRunRow(rawRun);
      if (!run) return undefined;
      const steps = new Map<string, StepOutcome>();
      for (const [cursor, v] of Object.entries(rawSteps)) steps.set(cursor, decodeStep(v));
      return { run, steps, signals: rawSignals.map(decodeSignal) };
    },

    loadRunRow,

    loadRunRows,

    async arriveAtJoin(parentRunId) {
      const res = await evalLua<number | null>(ARRIVE_LUA, [keys.run(parentRunId)], []);
      return res === null ? undefined : res;
    },

    async postSignal(runId, name, payload, opts) {
      const delivered = await evalLua<number>(
        POST_SIGNAL_LUA,
        [keys.sigIdem(runId), keys.inbox(runId), keys.job(runId), keys.queue],
        [opts?.idempotencyKey ?? "", JSON.stringify({ id: id(), name, payload }), runId, 0, 0],
      );
      return { delivered: delivered === 1 };
    },

    async markRunning(runId) {
      const res = await evalLua<number | null>(MARK_RUNNING_LUA, [keys.run(runId)], []);
      if (res === null) throw new Error(`markRunning: run ${runId} not found`);
      return res;
    },

    async checkpointStep(c: StepCheckpoint, fx?: Outbox) {
      const outcome: StepOutcome = {
        status: c.status,
        result: c.result,
        error: c.error,
        attempts: c.attempts,
        shape: c.shape,
      };
      const encoded = encodeStep(outcome);
      const res = await evalLua<[string, string?]>(
        CHECKPOINT_LUA,
        [
          keys.run(c.runId),
          keys.steps(c.runId),
          keys.inbox(c.runId),
          keys.queue,
          keys.runIndex,
          keys.timers,
          keys.seq,
          keys.idem,
        ],
        [c.cursorKey, encoded, serializeOutbox(fx)],
      );
      if (res[0] === "noRun") throw new Error(`checkpointStep: run ${c.runId} not found`);
      if (res[0] === "hit") return decodeStep(res[1] as string);
      return decodeStep(encoded);
    },

    async suspendRun(runId, status: SuspendStatus, fx?: Outbox) {
      const res = await evalLua<string>(
        SUSPEND_LUA,
        [
          keys.run(runId),
          keys.inbox(runId),
          keys.queue,
          keys.runIndex,
          keys.timers,
          keys.seq,
          keys.idem,
        ],
        [status, status !== "retrying" ? "1" : "0", serializeOutbox(fx)],
      );
      if (res === "noRun") throw new Error(`suspendRun: run ${runId} not found`);
    },

    async markTerminal(runId, outcome: TerminalOutcome, fx?: Outbox) {
      const hasOutput = outcome.status === "done" && outcome.output !== undefined;
      const hasError = outcome.status !== "done" && outcome.error !== undefined;
      const res = await evalLua<string>(
        MARK_TERMINAL_LUA,
        [
          keys.run(runId),
          keys.inbox(runId),
          keys.queue,
          keys.runIndex,
          keys.timers,
          keys.seq,
          keys.idem,
        ],
        [
          outcome.status,
          hasOutput ? "1" : "0",
          hasOutput ? JSON.stringify((outcome as { output: unknown }).output) : "",
          hasError ? "1" : "0",
          hasError ? JSON.stringify((outcome as { error: unknown }).error) : "",
          serializeOutbox(fx),
        ],
      );
      if (res === "noRun") throw new Error(`markTerminal: run ${runId} not found`);
    },

    async listRuns(filter: RunFilter, page: Page) {
      const statuses = statusList(filter.status);
      const max = page.cursor ? `(${page.cursor}` : "+inf";
      const ids = await client.zrevrangebyscore(keys.runIndex, max, "-inf");
      if (ids.length === 0) return { runs: [] };
      const pipe = client.pipeline();
      for (const runId of ids) pipe.hgetall(keys.run(runId));
      const res = await pipe.exec();
      const rows: { row: RunRow; seq: number }[] = [];
      for (let i = 0; i < ids.length && rows.length < page.limit; i++) {
        const h = (res?.[i]?.[1] ?? {}) as Hash;
        const row = toRunRow(h);
        if (!row) continue;
        if (statuses && !statuses.includes(row.status)) continue;
        if (filter.name && row.name !== filter.name) continue;
        if (filter.tag && !(row.tags?.includes(filter.tag) ?? false)) continue;
        rows.push({ row, seq: Number(h[RUN.seq]) });
      }
      const last = rows[rows.length - 1];
      const cursor = rows.length === page.limit && last ? String(last.seq) : undefined;
      return { runs: rows.map((r) => r.row), cursor };
    },

    async childrenOf(runId) {
      const ids = await client.smembers(keys.children(runId));
      const rows = await loadRunRows(ids);
      return rows.filter((r): r is RunRow => r !== undefined);
    },

    async runStats() {
      const stats = zeroRunStats();
      const ids = await client.zrange(keys.runIndex, 0, -1);
      if (ids.length === 0) return stats;
      const pipe = client.pipeline();
      for (const runId of ids) pipe.hget(keys.run(runId), RUN.status);
      const res = await pipe.exec();
      for (const r of res ?? []) {
        const s = r?.[1] as RunStatus | null;
        if (s) stats[s] += 1;
      }
      return stats;
    },

    async orphanedRuns(limit) {
      const ids = await client.zrange(keys.runIndex, 0, -1);
      if (ids.length === 0) return [];
      const hp = client.pipeline();
      const jp = client.pipeline();
      const tp = client.pipeline();
      for (const runId of ids) {
        hp.hgetall(keys.run(runId));
        jp.exists(keys.job(runId));
        tp.zscore(keys.timers, runId);
      }
      const [hr, jr, tr] = await Promise.all([hp.exec(), jp.exec(), tp.exec()]);
      const all: RunRow[] = [];
      const byId = new Map<string, RunRow>();
      const jobbed = new Set<string>();
      const timered = new Set<string>();
      ids.forEach((runId, i) => {
        const row = toRunRow((hr?.[i]?.[1] ?? {}) as Hash);
        if (row) {
          all.push(row);
          byId.set(runId, row);
        }
        if (jr?.[i]?.[1] === 1) jobbed.add(runId);
        if (tr?.[i]?.[1] != null) timered.add(runId);
      });
      const view: OrphanView = {
        hasJob: (runId) => jobbed.has(runId),
        hasTimer: (runId) => timered.has(runId),
        childrenOf: (runId) => all.filter((c) => c.parentRunId === runId),
        runById: (runId) => byId.get(runId),
      };
      return all
        .filter((r) => isOrphaned(r, view))
        .slice(0, limit)
        .map((r) => r.id);
    },

    async deleteRunsOlderThan(before, limit) {
      const cutoff = before.getTime();
      const ids = await client.zrange(keys.runIndex, 0, -1);
      if (ids.length === 0) return 0;
      const pipe = client.pipeline();
      for (const runId of ids) pipe.hgetall(keys.run(runId));
      const res = await pipe.exec();
      const victims: RunRow[] = [];
      for (let i = 0; i < ids.length && victims.length < limit; i++) {
        const row = toRunRow((res?.[i]?.[1] ?? {}) as Hash);
        if (row && isTerminal(row.status) && (row.createdAt?.getTime() ?? 0) < cutoff) {
          victims.push(row);
        }
      }
      if (victims.length === 0) return 0;
      const del = client.pipeline();
      for (const r of victims) {
        del.del(
          keys.run(r.id),
          keys.steps(r.id),
          keys.inbox(r.id),
          keys.job(r.id),
          keys.children(r.id),
          keys.sigIdem(r.id),
        );
        del.zrem(keys.runIndex, r.id);
        del.zrem(keys.timers, r.id);
        if (r.idempotencyKey !== undefined) {
          del.hdel(keys.idem, idemIdentity(r.name, r.version, r.idempotencyKey));
        }
      }
      await del.exec();
      return victims.length;
    },

    async retryRun(runId) {
      const res = await evalLua<number>(
        RETRY_LUA,
        [keys.run(runId), keys.job(runId), keys.queue],
        [runId, 0, 0],
      );
      if (res === -1) throw new Error(`retryRun: run ${runId} not found`);
      return { retried: res === 1 };
    },

    async upsertCron(spec: CronSpec) {
      await evalLua(
        UPSERT_CRON_LUA,
        [keys.crons, keys.cronsDue],
        [spec.name, encodeCron(cronRowFromSpec(spec, spec.nextRunAt)), spec.nextRunAt.getTime()],
      );
    },

    async dueCrons(now, limit) {
      const names = await client.zrangebyscore(
        keys.cronsDue,
        "-inf",
        now.getTime(),
        "LIMIT",
        0,
        limit,
      );
      if (names.length === 0) return [];
      const pipe = client.pipeline();
      for (const name of names) pipe.hget(keys.crons, name);
      const res = await pipe.exec();
      return names.flatMap((_, i) => {
        const raw = res?.[i]?.[1] as string | null;
        if (!raw) return [];
        const c = decodeCron(raw);
        return [
          {
            ...c,
            nextRunAt: new Date(c.nextRunAt),
            lastRunAt: c.lastRunAt === undefined ? undefined : new Date(c.lastRunAt),
          },
        ];
      });
    },

    async advanceCron(name, expectedNextRunAt, nextRunAt, lastRunAt) {
      const res = await evalLua<number>(
        ADVANCE_CRON_LUA,
        [keys.crons, keys.cronsDue],
        [
          name,
          expectedNextRunAt.getTime(),
          nextRunAt.getTime(),
          nextRunAt.toISOString(),
          lastRunAt.toISOString(),
        ],
      );
      return res === 1;
    },
  };
};
