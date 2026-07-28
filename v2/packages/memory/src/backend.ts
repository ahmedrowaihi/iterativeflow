import {
  type Backend,
  type ClaimOpts,
  type CronRow,
  type DeliveredSignal,
  type EnqueueOpts,
  type IdGen,
  type Lease,
  type Outbox,
  type OrphanView,
  type RunRow,
  type RunSpec,
  type StartResult,
  type StepCheckpoint,
  type StepOutcome,
  type SuspendStatus,
  type TerminalOutcome,
  type TimerDueOpts,
  createLocalWakeup,
  isOrphaned,
  isTerminal,
  newId,
  queueDepthOf,
  statusList,
  zeroRunStats,
} from "@iterativeflow/core/backend";

interface Job {
  runId: string;
  runAtMs: number;
  priority: number;
  version: number; // bumped on every enqueue; ack CASes on it to survive a mid-lease wake
  leaseToken?: string;
  leaseExpiresMs?: number;
}

const idemKey = (name: string, version: number, key: string): string =>
  JSON.stringify([name, version, key]);

const ms = (now?: Date): number => (now ?? new Date()).getTime();

/**
 * The in-memory reference backend: the four ports over one shared, single-threaded state.
 * Because every mutation core is synchronous, an outbox commits with no `await` boundary
 * in between — genuinely atomic, exactly what a Postgres `BEGIN…COMMIT` or a DynamoDB
 * `TransactWriteItems` buys on a real backend. It passes every `*Conformance` suite,
 * including `outboxConformance`, so it doubles as the oracle every real backend must match.
 */
export const createMemoryBackend = ({ id: idGen }: { id?: IdGen } = {}): Backend => {
  const id = idGen ?? newId;
  const runs = new Map<string, RunRow>();
  const idemIndex = new Map<string, string>();
  const steps = new Map<string, Map<string, StepOutcome>>();
  const jobs = new Map<string, Job>();
  const deadlines = new Map<string, number>();
  const signals = new Map<string, DeliveredSignal[]>(); // runId -> ordered inbox
  const signalIdem = new Set<string>(); // delivered signal idem keys `${runId} ${idemKey}` — dedups repeat delivery
  const runSeq = new Map<string, number>(); // runId -> insertion order (listing cursor)
  const crons = new Map<string, CronRow>();
  const joinRemaining = new Map<string, number>(); // parent runId -> children still to arrive at its join
  let seq = 0;
  let runCounter = 0;

  const insertRunCore = (spec: RunSpec, runId: string): void => {
    if (runs.has(runId)) return; // first-writer-wins by id (idempotent spawn on replay)
    runSeq.set(runId, ++runCounter);
    runs.set(runId, {
      id: runId,
      name: spec.name,
      version: spec.version,
      status: "pending",
      input: structuredClone(spec.input),
      attempts: 0,
      idempotencyKey: spec.idempotencyKey,
      tags: spec.tags ? [...spec.tags] : undefined,
      parentRunId: spec.parentRunId,
      parentCursorKey: spec.parentCursorKey,
      depth: spec.depth ?? 0,
      createdAt: spec.createdAt ?? new Date(),
    });
    steps.set(runId, new Map());
    if (spec.idempotencyKey) {
      idemIndex.set(idemKey(spec.name, spec.version, spec.idempotencyKey), runId);
    }
  };

  const enqueueCore = (runId: string, opts?: EnqueueOpts): void => {
    // Upsert keyed by runId; re-enqueue updates schedule/priority but never yanks an
    // active lease from the worker currently processing the run.
    const prior = jobs.get(runId);
    jobs.set(runId, {
      runId,
      runAtMs: opts?.runAt ? opts.runAt.getTime() : 0,
      priority: opts?.priority ?? 0,
      version: (prior?.version ?? 0) + 1,
      leaseToken: prior?.leaseToken,
      leaseExpiresMs: prior?.leaseExpiresMs,
    });
  };

  const scheduleCore = (runId: string, fireAt: Date): void => {
    deadlines.set(runId, fireAt.getTime()); // upsert: latest wins
  };

  const commitOutbox = (fx?: Outbox): void => {
    if (!fx) return;
    for (const s of fx.spawn ?? []) {
      insertRunCore(s.spec, s.runId);
      enqueueCore(s.runId, s.enqueue);
    }
    for (const e of fx.enqueue ?? []) enqueueCore(e.runId, e.opts);
    for (const t of fx.timers ?? []) scheduleCore(t.runId, t.fireAt);
    for (const runId of fx.cancelTimers ?? []) deadlines.delete(runId);
    for (const sigId of fx.consumeSignals ?? []) {
      for (const [runId, inbox] of signals) {
        const i = inbox.findIndex((s) => s.id === sigId);
        if (i >= 0) inbox.splice(i, 1);
        if (inbox.length === 0) signals.delete(runId);
      }
    }
    if (fx.joinTarget) joinRemaining.set(fx.joinTarget.runId, fx.joinTarget.count);
  };

  const startOne = (spec: RunSpec): StartResult => {
    if (spec.idempotencyKey) {
      const existing = idemIndex.get(idemKey(spec.name, spec.version, spec.idempotencyKey));
      if (existing) {
        const row = runs.get(existing);
        if (!row) throw new Error("startRun: idempotency index points at a missing run");
        return { runId: existing, created: false, status: row.status };
      }
    }
    const runId = id();
    insertRunCore(spec, runId);
    return { runId, created: true, status: "pending" };
  };

  const store: Backend["store"] = {
    async startRun(spec) {
      return startOne(spec);
    },

    async startManyRuns(specs) {
      return specs.map(startOne); // single-threaded → the whole batch lands or none does
    },

    async loadRun(runId) {
      const row = runs.get(runId);
      if (!row) return undefined;
      return {
        run: structuredClone(row),
        steps: new Map(
          [...(steps.get(runId) ?? new Map())].map(([k, v]) => [k, structuredClone(v)]),
        ),
        signals: (signals.get(runId) ?? []).map((s) => structuredClone(s)),
      };
    },

    async loadRunRow(runId) {
      const row = runs.get(runId);
      return row ? structuredClone(row) : undefined;
    },

    async loadRunRows(runIds) {
      return runIds.map((runId) => {
        const row = runs.get(runId);
        return row ? structuredClone(row) : undefined;
      });
    },

    async arriveAtJoin(parentRunId) {
      if (!runs.has(parentRunId)) return undefined;
      const n = (joinRemaining.get(parentRunId) ?? 0) - 1;
      joinRemaining.set(parentRunId, n);
      return n;
    },

    async postSignal(runId, name, payload, opts) {
      if (opts?.idempotencyKey) {
        const k = `${runId} ${opts.idempotencyKey}`;
        if (signalIdem.has(k)) return { delivered: false };
        signalIdem.add(k);
      }
      const inbox = signals.get(runId) ?? [];
      inbox.push({ id: id(), name, payload: structuredClone(payload) });
      signals.set(runId, inbox);
      enqueueCore(runId); // wake the parked run atomically with the delivery
      return { delivered: true };
    },

    async markRunning(runId) {
      const row = runs.get(runId);
      if (!row) throw new Error(`markRunning: run ${runId} not found`);
      if (isTerminal(row.status)) return row.attempts;
      row.attempts += 1;
      row.status = "running";
      return row.attempts;
    },

    async checkpointStep(c: StepCheckpoint, fx?: Outbox) {
      const stepMap = steps.get(c.runId);
      if (!stepMap) throw new Error(`checkpointStep: run ${c.runId} not found`);
      const existing = stepMap.get(c.cursorKey);
      if (existing) return structuredClone(existing); // first-writer-wins; skip the outbox
      const outcome: StepOutcome = {
        status: c.status,
        result: structuredClone(c.result),
        error: c.error ? structuredClone(c.error) : undefined,
        attempts: c.attempts,
        shape: c.shape,
      };
      stepMap.set(c.cursorKey, outcome);
      commitOutbox(fx); // atomic with the checkpoint, and only on the first write
      return structuredClone(outcome);
    },

    async suspendRun(runId, status: SuspendStatus, fx?: Outbox) {
      const row = runs.get(runId);
      if (!row) throw new Error(`suspendRun: run ${runId} not found`);
      if (isTerminal(row.status)) return; // already terminal — nothing to park
      row.status = status;
      if (status !== "retrying") row.attempts = 0;
      commitOutbox(fx);
    },

    async markTerminal(runId, outcome: TerminalOutcome, fx?: Outbox) {
      const row = runs.get(runId);
      if (!row) throw new Error(`markTerminal: run ${runId} not found`);
      if (row.status === "canceled") return; // cancel is sticky
      row.status = outcome.status;
      row.output = outcome.status === "done" ? outcome.output : undefined;
      row.error = outcome.status === "done" ? undefined : outcome.error;
      commitOutbox(fx);
    },

    async listRuns(filter, page) {
      const statuses = statusList(filter.status);
      const matched = [...runs.values()]
        .filter(
          (r) =>
            (!statuses || statuses.includes(r.status)) &&
            (!filter.name || r.name === filter.name) &&
            (!filter.tag || (r.tags?.includes(filter.tag) ?? false)),
        )
        .sort((a, b) => (runSeq.get(b.id) ?? 0) - (runSeq.get(a.id) ?? 0)); // newest first
      const before = page.cursor ? Number(page.cursor) : Number.POSITIVE_INFINITY;
      const rows = matched.filter((r) => (runSeq.get(r.id) ?? 0) < before).slice(0, page.limit);
      const last = rows[rows.length - 1];
      const cursor = rows.length === page.limit && last ? String(runSeq.get(last.id)) : undefined;
      return { runs: rows.map((r) => structuredClone(r)), cursor };
    },

    async childrenOf(runId) {
      return [...runs.values()]
        .filter((r) => r.parentRunId === runId)
        .map((r) => structuredClone(r));
    },

    async runStats() {
      const stats = zeroRunStats();
      for (const r of runs.values()) stats[r.status] += 1;
      return stats;
    },

    async orphanedRuns(limit) {
      const view: OrphanView = {
        hasJob: (runId) => jobs.has(runId),
        hasTimer: (runId) => deadlines.has(runId),
        childrenOf: (runId) => [...runs.values()].filter((c) => c.parentRunId === runId),
        runById: (runId) => runs.get(runId),
      };
      return [...runs.values()]
        .filter((r) => isOrphaned(r, view))
        .sort((a, b) => (runSeq.get(a.id) ?? 0) - (runSeq.get(b.id) ?? 0)) // oldest first
        .slice(0, limit)
        .map((r) => r.id);
    },

    async deleteRunsOlderThan(before, limit) {
      const cutoff = before.getTime();
      const victims = [...runs.values()]
        .filter((r) => isTerminal(r.status) && (r.createdAt?.getTime() ?? 0) < cutoff)
        .sort((a, b) => (runSeq.get(a.id) ?? 0) - (runSeq.get(b.id) ?? 0)) // oldest first
        .slice(0, limit)
        .map((r) => r.id);
      const gone = new Set(victims);
      for (const runId of victims) {
        const r = runs.get(runId);
        if (r?.idempotencyKey) idemIndex.delete(idemKey(r.name, r.version, r.idempotencyKey));
        runs.delete(runId);
        steps.delete(runId);
        signals.delete(runId);
        jobs.delete(runId);
        deadlines.delete(runId);
        joinRemaining.delete(runId);
        runSeq.delete(runId);
      }
      for (const k of signalIdem) if (gone.has(k.slice(0, k.indexOf(" ")))) signalIdem.delete(k);
      return victims.length;
    },

    async retryRun(runId) {
      const row = runs.get(runId);
      if (!row) throw new Error(`retryRun: run ${runId} not found`);
      if (row.status !== "failed") return { retried: false };
      row.status = "pending";
      row.error = undefined;
      enqueueCore(runId);
      return { retried: true };
    },

    async upsertCron(spec) {
      const existing = crons.get(spec.name);
      crons.set(spec.name, {
        name: spec.name,
        schedule: spec.schedule,
        flowName: spec.flowName,
        flowVersion: spec.flowVersion,
        input: structuredClone(spec.input),
        overlap: spec.overlap ?? "allow",
        nextRunAt: existing ? existing.nextRunAt : spec.nextRunAt, // keep timing on re-register
        lastRunAt: existing?.lastRunAt,
      });
    },

    async dueCrons(now, limit) {
      return [...crons.values()]
        .filter((c) => c.nextRunAt.getTime() <= now.getTime())
        .sort((a, b) => a.nextRunAt.getTime() - b.nextRunAt.getTime())
        .slice(0, limit)
        .map((c) => structuredClone(c));
    },

    async advanceCron(name, expectedNextRunAt, nextRunAt, lastRunAt) {
      const c = crons.get(name);
      if (!c || c.nextRunAt.getTime() !== expectedNextRunAt.getTime()) return false; // CAS lost
      c.nextRunAt = nextRunAt;
      c.lastRunAt = lastRunAt;
      return true;
    },
  };

  const queue: Backend["queue"] = {
    async enqueue(runId, opts) {
      enqueueCore(runId, opts);
    },

    async claim({ limit, leaseMs, now }: ClaimOpts) {
      const t = ms(now);
      const due = [...jobs.values()].filter(
        (j) => j.runAtMs <= t && (j.leaseExpiresMs === undefined || j.leaseExpiresMs <= t),
      );
      due.sort((a, b) => a.priority - b.priority || a.runAtMs - b.runAtMs);
      const leases: Lease[] = [];
      for (const j of due.slice(0, limit)) {
        const token = `${j.runId}#${++seq}`;
        j.leaseToken = token;
        j.leaseExpiresMs = t + leaseMs;
        leases.push({
          runId: j.runId,
          token,
          expiresAt: new Date(t + leaseMs),
          version: j.version,
        });
      }
      return leases;
    },

    async heartbeat(lease: Lease, { leaseMs, now }: { leaseMs: number; now?: Date }) {
      const t = ms(now);
      const j = jobs.get(lease.runId);
      if (
        !j ||
        j.leaseToken !== lease.token ||
        j.leaseExpiresMs === undefined ||
        j.leaseExpiresMs <= t
      ) {
        throw new Error(`heartbeat: lease for ${lease.runId} is no longer held`);
      }
      j.leaseExpiresMs = t + leaseMs;
      return { ...lease, expiresAt: new Date(t + leaseMs) };
    },

    async ack(lease: Lease, opts?: { now?: Date }) {
      const t = ms(opts?.now);
      const j = jobs.get(lease.runId);
      if (
        !j ||
        j.leaseToken !== lease.token ||
        j.leaseExpiresMs === undefined ||
        j.leaseExpiresMs <= t
      ) {
        return; // stale or expired lease — not ours to touch
      }
      if (j.version !== lease.version) {
        j.leaseToken = undefined;
        j.leaseExpiresMs = undefined;
        j.runAtMs = 0;
      } else {
        jobs.delete(lease.runId);
      }
    },

    async depth(now) {
      const jobsForDepth = [...jobs.values()].map((j) => ({
        runAt: j.runAtMs,
        leaseExpires: j.leaseExpiresMs,
      }));
      return queueDepthOf(jobsForDepth, ms(now));
    },
  };

  const timer: Backend["timer"] = {
    async schedule(runId, fireAt) {
      scheduleCore(runId, fireAt);
    },

    async dueBatch({ now, limit }: TimerDueOpts) {
      const t = ms(now);
      const due = [...deadlines.entries()]
        .filter(([, fireAtMs]) => fireAtMs <= t)
        .sort((a, b) => a[1] - b[1])
        .slice(0, limit)
        .map(([runId]) => runId);
      for (const runId of due) deadlines.delete(runId);
      return due;
    },

    async cancel(runId) {
      deadlines.delete(runId);
    },

    async nextDueAt(now) {
      const t = ms(now);
      let min: number | undefined;
      for (const fireAtMs of deadlines.values()) {
        if (fireAtMs > t && (min === undefined || fireAtMs < min)) min = fireAtMs;
      }
      return min === undefined ? null : new Date(min);
    },
  };

  return { store, queue, timer, wakeup: createLocalWakeup() };
};
