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
  type PurgeFilter,
  TERMINAL_STATUSES,
  ACTIVE_STATUSES,
  isOrphaned,
  isRunStatus,
  purgeMatcher,
  runSetStatuses,
  statusList,
  zeroRunStats,
} from "@iterativeflow/core/backend";
import type { RedisClient } from "#client";
import { JOB, type Keys, RUN } from "#keys";
import { ENQUEUE_FN, luaRunner, replyList, replyNumber, replyText } from "#scripts";
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

// Field names come from the pinned RUN/JOB maps; only status VALUES (`running`, `pending`, …) are
// literals here, exactly as in the memory oracle.
const IS_TERMINAL = TERMINAL_STATUSES.map((s) => `s == '${s}'`).join(" or ");

const TERMINAL_FN = `
local function iflow_terminal(s)
  return ${IS_TERMINAL}
end`;

const OUTBOX_LIB = `${ENQUEUE_FN}

local function iflow_apply(fx, qKey, idxKey, tmrKey, seqKey, idemKey, inboxKey)
  if fx.spawn then
    for _, s in ipairs(fx.spawn) do
      local seq = redis.call('INCR', seqKey)
      redis.call('HSET', s.runKey, unpack(s.fields))
      redis.call('HSET', s.runKey, '${RUN.seq}', seq)
      redis.call('ZADD', idxKey, seq, s.childId)
      if s.childrenKey then redis.call('SADD', s.childrenKey, s.childId) end
      if s.idemField then redis.call('HSET', idemKey, s.idemField, s.childId) end
      iflow_enqueue(qKey, s.childId, s.jobKey, s.runKey, s.runAtMs, s.priority)
    end
  end
  if fx.joinTarget then
    redis.call('HSET', fx.joinTarget.runKey, '${RUN.joinRemaining}', fx.joinTarget.count)
  end
  if fx.enqueue then
    for _, e in ipairs(fx.enqueue) do
      iflow_enqueue(qKey, e.runId, e.jobKey, e.runKey, e.runAtMs, e.priority)
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
iflow_enqueue(KEYS[4], ARGV[3], KEYS[3], KEYS[5], ARGV[4], ARGV[5])
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
if redis.call('HEXISTS', KEYS[2], ARGV[1]) == 1 then
  return {'hit', redis.call('HGET', KEYS[2], ARGV[1])}
end
if ARGV[4] ~= '' then
  if tonumber(redis.call('HGET', KEYS[9], '${JOB.version}')) ~= tonumber(ARGV[4]) then
    return {'skip'}
  end
end
redis.call('HSET', KEYS[2], ARGV[1], ARGV[2])
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

const MARK_TERMINAL_LUA = `${TERMINAL_FN}${OUTBOX_LIB}
local s = redis.call('HGET', KEYS[1], '${RUN.status}')
if s == false then return 'noRun' end
if iflow_terminal(s) then return 'ok' end
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
redis.call('HSET', KEYS[1], '${RUN.status}', 'pending', '${RUN.attempts}', '0')
redis.call('HDEL', KEYS[1], '${RUN.error}')
iflow_enqueue(KEYS[3], ARGV[1], KEYS[2], KEYS[1], ARGV[2], ARGV[3])
return 1`;

const UPSERT_CRON_LUA = `
local existing = redis.call('HGET', KEYS[1], ARGV[1])
local row, score = ARGV[2], ARGV[3]
if existing ~= false then
  local prev = cjson.decode(existing)
  local obj = cjson.decode(ARGV[2])
  obj.lastRunAt = prev.lastRunAt
  if prev.schedule == obj.schedule then
    obj.nextRunAt = prev.nextRunAt
    local zs = redis.call('ZSCORE', KEYS[2], ARGV[1])
    if zs ~= false then score = zs end
  end
  row = cjson.encode(obj)
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
  const evalLua = luaRunner(client);

  // Redis has no secondary index over runs, so a set operation scans the run index once and filters
  // in memory — same shape as deleteRuns here.
  const matchingRuns = async (
    filter: RunFilter,
    allowed: readonly RunStatus[],
    op: string,
    limit: number,
  ): Promise<RunRow[]> => {
    const statuses = new Set<string>(runSetStatuses(filter, allowed, op));
    if (statuses.size === 0) return [];
    const out: RunRow[] = [];
    for (const hash of await allRunHashes()) {
      if (out.length >= limit) break;
      const row = toRunRow(hash);
      if (
        row &&
        statuses.has(row.status) &&
        (filter.name === undefined || row.name === filter.name) &&
        (filter.version === undefined || row.version === filter.version) &&
        (filter.tag === undefined || (row.tags ?? []).includes(filter.tag))
      ) {
        out.push(row);
      }
    }
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
      runKey: keys.run(runId),
      runAtMs: opts?.runAt ? opts.runAt.getTime() : 0,
      priority: opts?.priority,
    });
    return JSON.stringify({
      spawn: fx.spawn?.map((s) => {
        const e = enq(s.runId, s.enqueue);
        return {
          runKey: keys.run(s.runId),
          childId: s.runId,
          fields: runFields(s.spec, s.runId),
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
      }),
      joinTarget: fx.joinTarget && {
        runKey: keys.run(fx.joinTarget.runId),
        count: fx.joinTarget.count,
      },
      enqueue: fx.enqueue?.map((x) => enq(x.runId, x.opts)),
      timers: fx.timers?.map((t) => ({ runId: t.runId, fireAtMs: t.fireAt.getTime() })),
      cancelTimers: fx.cancelTimers,
      consumeSignals: fx.consumeSignals,
    });
  };

  const loadRunRow = async (runId: string): Promise<RunRow | undefined> =>
    toRunRow(await client.hgetall(keys.run(runId)));

  // Fetched raw, so a scan that stops at its limit decodes only the runs it keeps.
  const allRunHashes = async (): Promise<Record<string, string>[]> =>
    Promise.all(
      (await client.zrange(keys.runIndex, 0, -1)).map((runId) => client.hgetall(keys.run(runId))),
    );

  const loadRunRows = async (runIds: readonly string[]): Promise<(RunRow | undefined)[]> =>
    Promise.all(runIds.map(loadRunRow));

  const startOne = async (spec: RunSpec): Promise<StartResult> => {
    const runId = id();
    const idemField =
      spec.idempotencyKey !== undefined
        ? idemIdentity(spec.name, spec.version, spec.idempotencyKey)
        : "";
    const childrenKey = spec.parentRunId !== undefined ? keys.children(spec.parentRunId) : "";
    const [verdict, existingId] = replyList(
      await evalLua(
        START_LUA,
        [keys.run(runId), keys.runIndex, keys.seq, keys.idem],
        [idemField, JSON.stringify(runFields(spec, runId)), runId, childrenKey],
      ),
    );
    if (verdict === "new") return { runId, created: true, status: "pending" };
    if (verdict !== "hit" || existingId === undefined) {
      throw new Error(`startRun: unexpected script verdict ${verdict}`);
    }
    const row = await loadRunRow(existingId);
    if (!row) throw new Error("startRun: idempotency index points at a missing run");
    return { runId: existingId, created: false, status: row.status };
  };

  const deleteRuns = async (filter: PurgeFilter, limit: number): Promise<number> => {
    const matches = purgeMatcher(filter);
    const victims: RunRow[] = [];
    for (const hash of await allRunHashes()) {
      if (victims.length >= limit) break;
      const row = toRunRow(hash);
      if (row && matches(row)) victims.push(row);
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
  };

  const store: Store = {
    startRun: startOne,

    async startManyRuns(specs) {
      return Promise.all(specs.map(startOne));
    },

    async loadRun(runId) {
      const [run, rawSteps, rawSignals] = await Promise.all([
        loadRunRow(runId),
        client.hgetall(keys.steps(runId)),
        client.lrange(keys.inbox(runId), 0, -1),
      ]);
      if (!run) return undefined;
      const steps = new Map<string, StepOutcome>();
      for (const [cursor, v] of Object.entries(rawSteps)) steps.set(cursor, decodeStep(v));
      return { run, steps, signals: rawSignals.map(decodeSignal) };
    },

    loadRunRow,

    loadRunRows,

    async arriveAtJoin(parentRunId) {
      return replyNumber(await evalLua(ARRIVE_LUA, [keys.run(parentRunId)], []));
    },

    async postSignal(runId, name, payload, opts) {
      const delivered = replyNumber(
        await evalLua(
          POST_SIGNAL_LUA,
          [keys.sigIdem(runId), keys.inbox(runId), keys.job(runId), keys.queue, keys.run(runId)],
          [opts?.idempotencyKey ?? "", JSON.stringify({ id: id(), name, payload }), runId, 0, ""],
        ),
      );
      return { delivered: delivered === 1 };
    },

    async markRunning(runId) {
      const res = replyNumber(await evalLua(MARK_RUNNING_LUA, [keys.run(runId)], []));
      if (res === undefined) throw new Error(`markRunning: run ${runId} not found`);
      return res;
    },

    async checkpointStep(c: StepCheckpoint, fx?: Outbox) {
      const outcome: StepOutcome = {
        status: c.status,
        result: c.result,
        error: c.error,
        attempts: c.attempts,
        call: c.call,
      };
      const encoded = encodeStep(outcome);
      const res = await evalLua(
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
          keys.job(c.runId),
        ],
        [
          c.cursorKey,
          encoded,
          serializeOutbox(fx),
          fx?.requireVersion !== undefined ? String(fx.requireVersion) : "",
        ],
      );
      const [verdict, stored] = replyList(res);
      if (verdict === "noRun") throw new Error(`checkpointStep: run ${c.runId} not found`);
      if (verdict === "hit" && stored !== undefined) return decodeStep(stored);
      if (verdict === "skip") return { status: c.status, attempts: c.attempts, committed: false };
      if (verdict === "ok") return decodeStep(encoded);
      throw new Error(`checkpointStep: unexpected script verdict ${verdict}`);
    },

    async suspendRun(runId, status: SuspendStatus, fx?: Outbox) {
      const res = await evalLua(
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
      if (replyText(res) === "noRun") throw new Error(`suspendRun: run ${runId} not found`);
    },

    async markTerminal(runId, outcome: TerminalOutcome, fx?: Outbox) {
      const output = outcome.status === "done" ? outcome.output : undefined;
      const error = outcome.status === "done" ? undefined : outcome.error;
      const res = await evalLua(
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
          output !== undefined ? "1" : "0",
          output !== undefined ? JSON.stringify(output) : "",
          error !== undefined ? "1" : "0",
          error !== undefined ? JSON.stringify(error) : "",
          serializeOutbox(fx),
        ],
      );
      if (replyText(res) === "noRun") throw new Error(`markTerminal: run ${runId} not found`);
    },

    async listRuns(filter: RunFilter, page: Page) {
      const statuses = statusList(filter.status);
      // Scan the index in bounded windows so an interactive page isn't O(total runs); a filtered
      // page has no secondary index, so widen the window to offset the misses.
      const filtered = Boolean(
        statuses || filter.name || filter.version !== undefined || filter.tag,
      );
      const window = filtered ? Math.max(page.limit * 4, 64) : page.limit;
      let max = page.cursor ? `(${page.cursor}` : "+inf";
      const rows: RunRow[] = [];
      let cursor: string | undefined;
      while (rows.length < page.limit) {
        const flat = await client.zrevrangebyscore(
          keys.runIndex,
          max,
          "-inf",
          "WITHSCORES",
          "LIMIT",
          0,
          window,
        );
        if (flat.length === 0) break;
        const ids = flat.filter((_, i) => i % 2 === 0);
        const scores = flat.filter((_, i) => i % 2 === 1).map(Number);
        const fetched = await loadRunRows(ids);
        for (let i = 0; i < ids.length && rows.length < page.limit; i++) {
          const row = fetched[i];
          if (!row) continue;
          if (statuses && !statuses.includes(row.status)) continue;
          if (filter.name && row.name !== filter.name) continue;
          if (filter.version !== undefined && row.version !== filter.version) continue;
          if (filter.tag && !(row.tags?.includes(filter.tag) ?? false)) continue;
          rows.push(row);
          cursor = String(scores[i]);
        }
        if (ids.length < window) break; // index exhausted
        max = `(${scores[scores.length - 1]}`;
      }
      return { runs: rows, cursor: rows.length === page.limit ? cursor : undefined };
    },

    async cancelRuns(filter, limit) {
      const victims = await matchingRuns(filter, ACTIVE_STATUSES, "cancelRuns", limit);
      for (const r of victims) {
        await store.markTerminal(r.id, { status: "canceled" }, { cancelTimers: [r.id] });
      }
      return victims.length;
    },

    async retryRuns(filter, limit) {
      const victims = await matchingRuns(filter, ["failed"], "retryRuns", limit);
      let n = 0;
      for (const r of victims) if ((await store.retryRun(r.id)).retried) n += 1;
      return n;
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
      const statuses = await Promise.all(
        ids.map((runId) => client.hget(keys.run(runId), RUN.status)),
      );
      for (const s of statuses) if (s !== null && isRunStatus(s)) stats[s] += 1;
      return stats;
    },

    async orphanedRuns(limit) {
      const ids = await client.zrange(keys.runIndex, 0, -1);
      if (ids.length === 0) return [];
      const [hr, jr, tr] = await Promise.all([
        loadRunRows(ids),
        Promise.all(ids.map((runId) => client.exists(keys.job(runId)))),
        Promise.all(ids.map((runId) => client.zscore(keys.timers, runId))),
      ]);
      const all: RunRow[] = [];
      const byId = new Map<string, RunRow>();
      const jobbed = new Set<string>();
      const timered = new Set<string>();
      ids.forEach((runId, i) => {
        const row = hr[i];
        if (row) {
          all.push(row);
          byId.set(runId, row);
        }
        if (jr[i] === 1) jobbed.add(runId);
        if (tr[i] !== null) timered.add(runId);
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

    deleteRuns,

    deleteRunsOlderThan: (before, limit) => deleteRuns({ before }, limit),

    async retryRun(runId) {
      const res = replyNumber(
        await evalLua(RETRY_LUA, [keys.run(runId), keys.job(runId), keys.queue], [runId, 0, ""]),
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
      const raws = await client.hmget(keys.crons, ...names);
      return raws.flatMap((raw) => (raw ? [decodeCron(raw)] : []));
    },

    async listCrons() {
      const all = await client.hgetall(keys.crons);
      return Object.values(all)
        .map(decodeCron)
        .sort((a, b) => a.name.localeCompare(b.name));
    },

    async removeCron(name) {
      const removed = await client.hdel(keys.crons, name);
      await client.zrem(keys.cronsDue, name);
      return removed > 0;
    },

    async dueCronCount(now, names) {
      const wanted = names && new Set(names);
      if (!wanted) return client.zcount(keys.cronsDue, "-inf", now.getTime());
      const due = await client.zrangebyscore(keys.cronsDue, "-inf", now.getTime());
      if (due.length === 0) return 0;
      const raws = await client.hmget(keys.crons, ...due);
      return raws.filter((raw) => raw && wanted.has(decodeCron(raw).flowName)).length;
    },

    async advanceCron(name, expectedNextRunAt, nextRunAt, lastRunAt) {
      const res = await evalLua(
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
      return replyNumber(res) === 1;
    },
  };
  return store;
};
