import {
  type CronSpec,
  type IdGen,
  type Outbox,
  type RunFilter,
  type RunRow,
  type RunSpec,
  type StartResult,
  type StepCheckpoint,
  type StepOutcome,
  type Store,
  type SuspendStatus,
  type TerminalOutcome,
  type OrphanView,
  type Page,
  type PurgeFilter,
  type RunSelection,
  TERMINAL_STATUSES,
  type RunStatus,
  cancelSelection,
  enqueueFields,
  isOrphaned,
  isRunStatus,
  listSelection,
  purgeSelection,
  runMatcher,
  resetsAttempts,
  retrySelection,
  shardAdmits,
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
  encodeSignal,
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

// Insert-if-absent: a replayed spawn re-issues the same child id, and the first write wins.
const INSERT_RUN_FN = `
local function iflow_insert_run(s, idxKey, seqKey, idemKey)
  if redis.call('EXISTS', s.runKey) == 1 then return end
  local seq = redis.call('INCR', seqKey)
  redis.call('HSET', s.runKey, unpack(s.fields))
  redis.call('HSET', s.runKey, '${RUN.seq}', seq)
  redis.call('ZADD', idxKey, seq, s.runId)
  if s.childrenKey then redis.call('SADD', s.childrenKey, s.runId) end
  if s.idemField then redis.call('HSET', idemKey, s.idemField, s.runId) end
end`;

// KEYS[1..6] are the `outboxScriptKeys` prefix; an outbox script's own keys start at KEYS[7].
const OUTBOX_LIB = `${ENQUEUE_FN}${INSERT_RUN_FN}

local function iflow_apply(fx)
  local inboxKey, qKey, idxKey, tmrKey, seqKey, idemKey = KEYS[1], KEYS[2], KEYS[3], KEYS[4], KEYS[5], KEYS[6]
  if fx.spawn then
    for _, s in ipairs(fx.spawn) do
      iflow_insert_run(s, idxKey, seqKey, idemKey)
      iflow_enqueue(qKey, s.runId, s.jobKey, s.runKey, s.runAtMs, s.priority)
    end
  end
  if fx.joinTarget then
    redis.call('HSET', fx.joinTarget.runKey, '${RUN.joinRemaining}', fx.joinTarget.count)
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

const START_LUA = `${INSERT_RUN_FN}
local s = cjson.decode(ARGV[1])
if s.idemField then
  local existing = redis.call('HGET', KEYS[3], s.idemField)
  if existing then return {'hit', existing} end
end
iflow_insert_run(s, KEYS[1], KEYS[2], KEYS[3])
return {'new'}`;

const POST_SIGNAL_LUA = `${ENQUEUE_FN}
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
local runKey, stepsKey, jobKey = KEYS[7], KEYS[8], KEYS[9]
if redis.call('EXISTS', runKey) == 0 then return {'noRun'} end
if redis.call('HEXISTS', stepsKey, ARGV[1]) == 1 then
  return {'hit', redis.call('HGET', stepsKey, ARGV[1])}
end
if ARGV[4] ~= '' then
  if tonumber(redis.call('HGET', jobKey, '${JOB.version}')) ~= tonumber(ARGV[4]) then
    return {'skip'}
  end
end
redis.call('HSET', stepsKey, ARGV[1], ARGV[2])
if ARGV[3] ~= '' then iflow_apply(cjson.decode(ARGV[3])) end
return {'ok'}`;

const SUSPEND_LUA = `${TERMINAL_FN}${OUTBOX_LIB}
local runKey = KEYS[7]
local s = redis.call('HGET', runKey, '${RUN.status}')
if s == false then return 'noRun' end
if iflow_terminal(s) then return 'ok' end
redis.call('HSET', runKey, '${RUN.status}', ARGV[1])
if ARGV[2] == '1' then redis.call('HSET', runKey, '${RUN.attempts}', 0) end
if ARGV[3] ~= '' then iflow_apply(cjson.decode(ARGV[3])) end
return 'ok'`;

const MARK_TERMINAL_LUA = `${TERMINAL_FN}${OUTBOX_LIB}
local runKey = KEYS[7]
local s = redis.call('HGET', runKey, '${RUN.status}')
if s == false then return 'noRun' end
if iflow_terminal(s) then return 'terminal' end
redis.call('HSET', runKey, '${RUN.status}', ARGV[1])
if ARGV[2] == '1' then redis.call('HSET', runKey, '${RUN.output}', ARGV[3])
else redis.call('HDEL', runKey, '${RUN.output}') end
if ARGV[4] == '1' then redis.call('HSET', runKey, '${RUN.error}', ARGV[5])
else redis.call('HDEL', runKey, '${RUN.error}') end
if ARGV[6] ~= '' then iflow_apply(cjson.decode(ARGV[6])) end
return 'ok'`;

const RETRY_LUA = `${ENQUEUE_FN}
if redis.call('HGET', KEYS[1], '${RUN.status}') ~= 'failed' then return 0 end
redis.call('HSET', KEYS[1], '${RUN.status}', 'pending', '${RUN.attempts}', '0')
redis.call('HDEL', KEYS[1], '${RUN.error}')
iflow_enqueue(KEYS[3], ARGV[1], KEYS[2], KEYS[1], ARGV[2], ARGV[3])
return 1`;

// KEYS[1..4] = runIndex, timers, queue, idem; KEYS[5] the run hash, then its other per-run keys,
// then — when ARGV[3] is '1' — the parent's run hash, which must not be live.
// ARGV = runId, idem field ('' when unkeyed), parent guard flag, then the statuses the purge admits.
const DELETE_RUN_LUA = `${TERMINAL_FN}
local s = redis.call('HGET', KEYS[5], '${RUN.status}')
local admitted = false
for i = 4, #ARGV do
  if ARGV[i] == s then admitted = true end
end
if not admitted then return 0 end
local last = #KEYS
if ARGV[3] == '1' then
  local p = redis.call('HGET', KEYS[last], '${RUN.status}')
  if p ~= false and not iflow_terminal(p) then return 0 end
  last = last - 1
end
for i = 5, last do redis.call('DEL', KEYS[i]) end
redis.call('ZREM', KEYS[1], ARGV[1])
redis.call('ZREM', KEYS[2], ARGV[1])
redis.call('ZREM', KEYS[3], ARGV[1])
if ARGV[2] ~= '' then redis.call('HDEL', KEYS[4], ARGV[2]) end
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

  // Redis has no secondary index over runs, so a set operation scans the run index (oldest first)
  // and filters in memory.
  const scanRuns = async (sel: RunSelection, limit: number): Promise<RunRow[]> => {
    if (sel.statuses.length === 0) return [];
    const hashes = await allRunHashes();
    const statuses = new Map<string, RunStatus>();
    for (const h of hashes) {
      const status = h[RUN.status];
      if (h[RUN.id] && status !== undefined && isRunStatus(status)) statuses.set(h[RUN.id], status);
    }
    const matches = runMatcher(sel, (runId) => statuses.get(runId));
    const out: RunRow[] = [];
    for (const hash of hashes) {
      if (out.length >= limit) break;
      const row = toRunRow(hash);
      if (row && matches(row)) out.push(row);
    }
    return out;
  };

  const insertSpec = (spec: RunSpec, runId: string) => ({
    runId,
    runKey: keys.run(runId),
    fields: runFields(spec, runId),
    childrenKey: spec.parentRunId !== undefined ? keys.children(spec.parentRunId) : undefined,
    idemField:
      spec.idempotencyKey !== undefined
        ? idemIdentity(spec.name, spec.version, spec.idempotencyKey)
        : undefined,
  });

  const outboxScriptKeys = (runId: string, ...own: string[]): string[] => [
    keys.inbox(runId),
    keys.queue,
    keys.runIndex,
    keys.timers,
    keys.seq,
    keys.idem,
    ...own,
  ];

  // Prebuild every dynamic key + child hash in JS, so the Lua only applies what it's handed — keeping
  // the atomic domain single-node (per-run keys are hash-tagged but the shared keys are not).
  const serializeOutbox = (fx?: Outbox): string => {
    if (!fx) return "";
    return JSON.stringify({
      spawn: fx.spawn?.map((s) => ({
        ...insertSpec(s.spec, s.runId),
        jobKey: keys.job(s.runId),
        ...enqueueFields(undefined, s.spec.priority),
      })),
      joinTarget: fx.joinTarget && {
        runKey: keys.run(fx.joinTarget.runId),
        count: fx.joinTarget.count,
      },
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
    const [verdict, existingId] = replyList(
      await evalLua(
        START_LUA,
        [keys.runIndex, keys.seq, keys.idem],
        [JSON.stringify(insertSpec(spec, runId))],
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
    const sel = purgeSelection(filter);
    const victims = await scanRuns(sel, limit);
    const guardsParent = (r: RunRow): r is RunRow & { parentRunId: string } =>
      sel.spareChildrenOfLiveParents === true && r.parentRunId !== undefined;
    const deleted = await Promise.all(
      victims.map(async (r) =>
        replyNumber(
          await evalLua(
            DELETE_RUN_LUA,
            [
              keys.runIndex,
              keys.timers,
              keys.queue,
              keys.idem,
              ...keys.perRun(r.id),
              ...(guardsParent(r) ? [keys.run(r.parentRunId)] : []),
            ],
            [
              r.id,
              r.idempotencyKey === undefined
                ? ""
                : idemIdentity(r.name, r.version, r.idempotencyKey),
              guardsParent(r) ? "1" : "0",
              ...sel.statuses,
            ],
          ),
        ),
      ),
    );
    return deleted.filter((n) => n === 1).length;
  };

  const markTerminal = async (runId: string, outcome: TerminalOutcome, fx?: Outbox) => {
    const output = outcome.status === "done" ? outcome.output : undefined;
    const error = outcome.status === "done" ? undefined : outcome.error;
    const res = await evalLua(MARK_TERMINAL_LUA, outboxScriptKeys(runId, keys.run(runId)), [
      outcome.status,
      output !== undefined ? "1" : "0",
      output !== undefined ? JSON.stringify(output) : "",
      error !== undefined ? "1" : "0",
      error !== undefined ? JSON.stringify(error) : "",
      serializeOutbox(fx),
    ]);
    if (replyText(res) === "noRun") throw new Error(`markTerminal: run ${runId} not found`);
    return replyText(res) === "ok";
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
          [opts?.idempotencyKey ?? "", encodeSignal({ id: id(), name, payload }), runId, 0, ""],
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
        outboxScriptKeys(c.runId, keys.run(c.runId), keys.steps(c.runId), keys.job(c.runId)),
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
      const res = await evalLua(SUSPEND_LUA, outboxScriptKeys(runId, keys.run(runId)), [
        status,
        resetsAttempts(status) ? "1" : "0",
        serializeOutbox(fx),
      ]);
      if (replyText(res) === "noRun") throw new Error(`suspendRun: run ${runId} not found`);
    },

    markTerminal,

    async listRuns(filter: RunFilter, page: Page) {
      const matches = runMatcher(listSelection(filter));
      // Scan the index in bounded windows so an interactive page isn't O(total runs); a filtered
      // page has no secondary index, so widen the window to offset the misses.
      const filtered = Object.values(filter).some((v) => v !== undefined);
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
          if (!row || !matches(row)) continue;
          rows.push(row);
          cursor = String(scores[i]);
        }
        if (ids.length < window) break; // index exhausted
        max = `(${scores[scores.length - 1]}`;
      }
      return { runs: rows, cursor: rows.length === page.limit ? cursor : undefined };
    },

    async cancelRuns(filter, limit) {
      const victims = await scanRuns(cancelSelection(filter), limit);
      let n = 0;
      for (const r of victims) {
        if (await markTerminal(r.id, { status: "canceled" }, { cancelTimers: [r.id] })) n += 1;
      }
      return n;
    },

    async retryRuns(filter, limit) {
      const victims = await scanRuns(retrySelection(filter), limit);
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

    async retryRun(runId) {
      const res = replyNumber(
        await evalLua(RETRY_LUA, [keys.run(runId), keys.job(runId), keys.queue], [runId, 0, ""]),
      );
      return { retried: res === 1 };
    },

    async upsertCron(spec: CronSpec) {
      await evalLua(
        UPSERT_CRON_LUA,
        [keys.crons, keys.cronsDue],
        [spec.name, encodeCron(cronRowFromSpec(spec, spec.nextRunAt)), spec.nextRunAt.getTime()],
      );
    },

    async nextCronAt() {
      const [, score] = await client.zrange(keys.cronsDue, 0, 0, "WITHSCORES");
      return score === undefined ? null : new Date(Number(score));
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
      if (!names) return client.zcount(keys.cronsDue, "-inf", now.getTime());
      const due = await client.zrangebyscore(keys.cronsDue, "-inf", now.getTime());
      if (due.length === 0) return 0;
      const raws = await client.hmget(keys.crons, ...due);
      return raws.filter((raw) => raw !== null && shardAdmits(names, decodeCron(raw).flowName))
        .length;
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
