import {
  type EnqueueOpts,
  type IdGen,
  type Outbox,
  type OrphanView,
  type RunSpec,
  type StartResult,
  type StepOutcome,
  type Store,
  type SuspendStatus,
  TERMINAL_STATUSES,
  isOrphaned,
  isTerminal,
  statusList,
  zeroRunStats,
} from "@iterativeflow/core/backend";
import { type ClientSession, type Db, type Filter, type MongoClient, ObjectId } from "mongodb";
import type { Names } from "#collections";
import {
  type CronDoc,
  type RunDoc,
  type SignalDoc,
  type StepDoc,
  buildRunDoc,
  mapCron,
  mapRun,
  mapSignal,
  mapStep,
} from "#codec";

const isDup = (e: unknown): boolean =>
  typeof e === "object" && e !== null && (e as { code?: number }).code === 11000;

/**
 * The MongoDB {@link Store}: runs/steps/signals/crons as documents, the outbox committed across
 * collections in one multi-document session transaction (so the deployment must be a replica set).
 * Mirrors the in-memory reference backend's semantics — idempotent starts, first-writer-wins
 * checkpoints, terminal/cancel stickiness, poison-pill reset on forward progress.
 */
export const createMongoStore = (client: MongoClient, db: Db, n: Names, id: IdGen): Store => {
  const runs = db.collection<RunDoc>(n.runs);
  const steps = db.collection<StepDoc>(n.steps);
  const signals = db.collection<SignalDoc>(n.signals);
  const crons = db.collection<CronDoc>(n.crons);
  const jobs = db.collection<{ _id: string; run_at: number; priority: number; version: number }>(
    n.jobs,
  );
  const timers = db.collection<{ _id: string; fire_at: number }>(n.timers);

  const inTx = async <T>(fn: (session: ClientSession) => Promise<T>): Promise<T> => {
    const session = client.startSession();
    try {
      return await session.withTransaction(fn);
    } finally {
      await session.endSession();
    }
  };

  const enqueue = (runId: string, opts: EnqueueOpts | undefined, session: ClientSession) =>
    jobs.updateOne(
      { _id: runId },
      {
        $set: { run_at: opts?.runAt ? opts.runAt.getTime() : 0, priority: opts?.priority ?? 0 },
        $inc: { version: 1 },
      },
      { upsert: true, session },
    );

  const insertSpawn = async (
    spec: RunSpec,
    runId: string,
    session: ClientSession,
  ): Promise<void> => {
    try {
      await runs.insertOne(buildRunDoc(spec, runId, new ObjectId()), { session });
    } catch (e) {
      if (!isDup(e)) throw e; // insert-by-id is first-writer-wins: replay re-issues the same spawn
    }
  };

  const commitOutbox = async (fx: Outbox | undefined, session: ClientSession): Promise<void> => {
    if (!fx) return;
    for (const s of fx.spawn ?? []) {
      await insertSpawn(s.spec, s.runId, session);
      await enqueue(s.runId, s.enqueue, session);
    }
    for (const e of fx.enqueue ?? []) await enqueue(e.runId, e.opts, session);
    for (const t of fx.timers ?? []) {
      await timers.updateOne(
        { _id: t.runId },
        { $set: { fire_at: t.fireAt.getTime() } },
        { upsert: true, session },
      );
    }
    if (fx.cancelTimers?.length) {
      await timers.deleteMany({ _id: { $in: [...fx.cancelTimers] } }, { session });
    }
    if (fx.consumeSignals?.length) {
      await signals.deleteMany({ _id: { $in: [...fx.consumeSignals] } }, { session });
    }
    if (fx.joinTarget) {
      await runs.updateOne(
        { _id: fx.joinTarget.runId },
        { $set: { join_remaining: fx.joinTarget.count } },
        { session },
      );
    }
  };

  const startOne = async (spec: RunSpec): Promise<StartResult> => {
    const runId = id();
    try {
      await runs.insertOne(buildRunDoc(spec, runId, new ObjectId()));
      return { runId, created: true, status: "pending" };
    } catch (e) {
      if (!isDup(e)) throw e;
      const existing = await runs.findOne({
        name: spec.name,
        version: spec.version,
        idempotency_key: spec.idempotencyKey,
      });
      if (!existing)
        throw new Error(`startRun: idempotency collision without a matching run`, { cause: e });
      return { runId: existing._id, created: false, status: existing.status };
    }
  };

  return {
    startRun: startOne,

    async startManyRuns(specs) {
      return Promise.all(specs.map(startOne));
    },

    async loadRun(runId) {
      const run = await runs.findOne({ _id: runId });
      if (!run) return undefined;
      const [stepDocs, signalDocs] = await Promise.all([
        steps.find({ run_id: runId }).toArray(),
        signals.find({ run_id: runId }).sort({ ord: 1 }).toArray(),
      ]);
      const stepMap = new Map<string, StepOutcome>();
      for (const s of stepDocs) stepMap.set(s.cursor_key, mapStep(s));
      return { run: mapRun(run), steps: stepMap, signals: signalDocs.map(mapSignal) };
    },

    async loadRunRow(runId) {
      const run = await runs.findOne({ _id: runId });
      return run ? mapRun(run) : undefined;
    },

    async loadRunRows(runIds) {
      if (runIds.length === 0) return [];
      const docs = await runs.find({ _id: { $in: [...runIds] } }).toArray();
      const byId = new Map(docs.map((d) => [d._id, d]));
      return runIds.map((rid) => {
        const d = byId.get(rid);
        return d ? mapRun(d) : undefined;
      });
    },

    async arriveAtJoin(parentRunId) {
      const updated = await runs.findOneAndUpdate(
        { _id: parentRunId },
        { $inc: { join_remaining: -1 } },
        { returnDocument: "after" },
      );
      return updated ? (updated.join_remaining ?? 0) : undefined;
    },

    async postSignal(runId, name, payload, opts) {
      const signalDoc: SignalDoc = {
        _id: id(),
        run_id: runId,
        name,
        payload,
        ord: new ObjectId(),
        ...(opts?.idempotencyKey !== undefined && { idem_key: opts.idempotencyKey }),
      };
      try {
        return await inTx(async (session) => {
          await signals.insertOne(signalDoc, { session });
          await enqueue(runId, undefined, session); // wake the parked run atomically with delivery
          return { delivered: true };
        });
      } catch (e) {
        if (isDup(e)) return { delivered: false }; // idempotent re-delivery on the idem index
        throw e;
      }
    },

    async markRunning(runId) {
      return inTx(async (session) => {
        const run = await runs.findOne({ _id: runId }, { session });
        if (!run) throw new Error(`markRunning: run ${runId} not found`);
        if (isTerminal(run.status)) return run.attempts; // terminal — never resurrect
        await runs.updateOne(
          { _id: runId },
          { $set: { status: "running" }, $inc: { attempts: 1 } },
          { session },
        );
        return run.attempts + 1;
      });
    },

    async checkpointStep(c, fx) {
      const stepDoc: StepDoc = {
        _id: `${c.runId}:${c.cursorKey}`,
        run_id: c.runId,
        cursor_key: c.cursorKey,
        status: c.status,
        attempts: c.attempts,
        ...(c.result !== undefined && { result: c.result }),
        ...(c.error !== undefined && { error: c.error }),
        ...(c.shape !== undefined && { shape: c.shape }),
      };
      try {
        return await inTx(async (session) => {
          const run = await runs.findOne({ _id: c.runId }, { session });
          if (!run) throw new Error(`checkpointStep: run ${c.runId} not found`);
          if (fx?.requireVersion !== undefined) {
            const existing = await steps.findOne({ _id: stepDoc._id }, { session });
            if (existing) return mapStep(existing);
            const touched = await jobs.updateOne(
              { _id: c.runId, version: fx.requireVersion },
              { $set: { version: fx.requireVersion } },
              { session },
            );
            if (touched.matchedCount === 0) {
              return { status: c.status, attempts: c.attempts, committed: false };
            }
          }
          await steps.insertOne(stepDoc, { session });
          await commitOutbox(fx, session); // atomic with the checkpoint, only on the first write
          return mapStep(stepDoc);
        });
      } catch (e) {
        if (!isDup(e)) throw e;
        const existing = await steps.findOne({ _id: stepDoc._id }); // first-writer-wins; skip outbox
        if (!existing)
          throw new Error(`checkpointStep: step ${stepDoc._id} vanished`, { cause: e });
        return mapStep(existing);
      }
    },

    async suspendRun(runId, status: SuspendStatus, fx) {
      await inTx(async (session) => {
        const run = await runs.findOne({ _id: runId }, { session });
        if (!run) throw new Error(`suspendRun: run ${runId} not found`);
        if (isTerminal(run.status)) return; // already terminal — nothing to park
        const set: Record<string, unknown> = { status };
        if (status !== "retrying") set.attempts = 0; // forward progress resets the poison-pill cap
        await runs.updateOne({ _id: runId }, { $set: set }, { session });
        await commitOutbox(fx, session);
      });
    },

    async markTerminal(runId, outcome, fx) {
      await inTx(async (session) => {
        const run = await runs.findOne({ _id: runId }, { session });
        if (!run) throw new Error(`markTerminal: run ${runId} not found`);
        if (run.status === "canceled") return; // cancel is sticky
        const output = outcome.status === "done" ? outcome.output : undefined;
        const error = outcome.status === "done" ? undefined : outcome.error;
        const set: Record<string, unknown> = { status: outcome.status };
        const unset: Record<string, ""> = {};
        if (output === undefined) unset.output = "";
        else set.output = output;
        if (error === undefined) unset.error = "";
        else set.error = error;
        const update: Record<string, unknown> = { $set: set };
        if (Object.keys(unset).length) update.$unset = unset;
        await runs.updateOne({ _id: runId }, update, { session });
        await commitOutbox(fx, session);
      });
    },

    async listRuns(filter, page) {
      const statuses = statusList(filter.status);
      const q: Filter<RunDoc> = {
        ...(statuses && { status: { $in: statuses } }),
        ...(filter.name && { name: filter.name }),
        ...(filter.tag && { tags: filter.tag }),
        ...(page.cursor && { ord: { $lt: new ObjectId(page.cursor) } }),
      };
      const docs = await runs.find(q).sort({ ord: -1 }).limit(page.limit).toArray();
      const last = docs[docs.length - 1];
      const cursor = docs.length === page.limit && last ? last.ord.toHexString() : undefined;
      return { runs: docs.map(mapRun), cursor };
    },

    async childrenOf(runId) {
      const docs = await runs.find({ parent_run_id: runId }).toArray();
      return docs.map(mapRun);
    },

    async runStats() {
      const stats = zeroRunStats();
      const grouped = await runs
        .aggregate<{ _id: keyof typeof stats; count: number }>([
          { $group: { _id: "$status", count: { $sum: 1 } } },
        ])
        .toArray();
      for (const g of grouped) stats[g._id] = g.count;
      return stats;
    },

    async deleteRunsOlderThan(before, limit) {
      const victims = await runs
        .find({ status: { $in: [...TERMINAL_STATUSES] }, created_at: { $lt: before.getTime() } })
        .sort({ ord: 1 })
        .limit(limit)
        .toArray();
      if (victims.length === 0) return 0;
      const ids = victims.map((r) => r._id);
      await Promise.all([
        steps.deleteMany({ run_id: { $in: ids } }),
        signals.deleteMany({ run_id: { $in: ids } }),
        jobs.deleteMany({ _id: { $in: ids } }),
        timers.deleteMany({ _id: { $in: ids } }),
        runs.deleteMany({ _id: { $in: ids } }),
      ]);
      return victims.length;
    },

    async orphanedRuns(limit) {
      const runDocs = await runs.find({}).sort({ ord: 1 }).toArray();
      const ids = runDocs.map((r) => r._id);
      const [jobDocs, timerDocs] = await Promise.all([
        jobs
          .find({ _id: { $in: ids } })
          .project({ _id: 1 })
          .toArray(),
        timers
          .find({ _id: { $in: ids } })
          .project({ _id: 1 })
          .toArray(),
      ]);
      const hasJob = new Set(jobDocs.map((j) => j._id as string));
      const hasTimer = new Set(timerDocs.map((t) => t._id as string));
      const asOrphan = (r: RunDoc) => ({
        id: r._id,
        status: r.status,
        parentRunId: r.parent_run_id,
      });
      const byId = new Map(runDocs.map((r) => [r._id, r]));
      const view: OrphanView = {
        hasJob: (rid) => hasJob.has(rid),
        hasTimer: (rid) => hasTimer.has(rid),
        childrenOf: (rid) => runDocs.filter((c) => c.parent_run_id === rid).map(asOrphan),
        runById: (rid) => {
          const r = byId.get(rid);
          return r ? asOrphan(r) : undefined;
        },
      };
      return runDocs
        .filter((r) => isOrphaned(asOrphan(r), view))
        .slice(0, limit)
        .map((r) => r._id);
    },

    async retryRun(runId) {
      return inTx(async (session) => {
        const res = await runs.updateOne(
          { _id: runId, status: "failed" },
          { $set: { status: "pending" }, $unset: { error: "" } },
          { session },
        );
        const retried = res.modifiedCount === 1;
        if (retried) await enqueue(runId, undefined, session);
        return { retried };
      });
    },

    async upsertCron(spec) {
      await crons.updateOne(
        { _id: spec.name },
        {
          $set: {
            schedule: spec.schedule,
            flow_name: spec.flowName,
            flow_version: spec.flowVersion,
            input: spec.input,
            overlap: spec.overlap ?? "allow",
          },
          // Keep the existing schedule timing when re-registering an already-known cron.
          $setOnInsert: { next_run_at: spec.nextRunAt.getTime(), last_run_at: null },
        },
        { upsert: true },
      );
    },

    async dueCrons(now, limit) {
      const docs = await crons
        .find({ next_run_at: { $lte: now.getTime() } })
        .sort({ next_run_at: 1 })
        .limit(limit)
        .toArray();
      return docs.map(mapCron);
    },

    async advanceCron(name, expectedNextRunAt, nextRunAt, lastRunAt) {
      const res = await crons.updateOne(
        { _id: name, next_run_at: expectedNextRunAt.getTime() },
        { $set: { next_run_at: nextRunAt.getTime(), last_run_at: lastRunAt.getTime() } },
      );
      return res.modifiedCount === 1;
    },
  };
};
