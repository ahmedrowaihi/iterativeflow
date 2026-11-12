import {
  type EnqueueOpts,
  type IdGen,
  type Outbox,
  type OrphanView,
  type PurgeFilter,
  type RunSelection,
  type RunSpec,
  type StartResult,
  type StepOutcome,
  type Store,
  type SuspendStatus,
  TERMINAL_STATUSES,
  cancelSelection,
  durable,
  isOrphaned,
  isTerminal,
  listSelection,
  purgeSelection,
  resetsAttempts,
  retrySelection,
  zeroRunStats,
} from "@iterativeflow/core/backend";
import { type ClientSession, type Db, type Filter, type MongoClient, ObjectId } from "mongodb";
import type { Names } from "#collections";
import {
  type CronDoc,
  type RunDoc,
  type SignalDoc,
  type StepDoc,
  type TimerDoc,
  buildRunDoc,
  durableError,
  mapCron,
  mapRun,
  mapSignal,
  mapStep,
} from "#codec";
import { type JobDoc, enqueueJobs } from "#queue";

const selectionFilter = (sel: RunSelection): Filter<RunDoc> => ({
  status: { $in: [...sel.statuses] },
  ...(sel.name !== undefined && { name: sel.name }),
  ...(sel.version !== undefined && { version: sel.version }),
  ...(sel.tag !== undefined && { tags: sel.tag }),
  ...(sel.before !== undefined && { created_at: { $lt: sel.before.getTime() } }),
});

/**
 * The MongoDB {@link Store}: runs/steps/signals/crons as documents, the outbox committed across
 * collections in one multi-document session transaction (so the deployment must be a replica set).
 * Mirrors the in-memory reference backend's semantics — idempotent starts, first-writer-wins
 * checkpoints, terminal/cancel stickiness, poison-pill reset on forward progress.
 */
export const createMongoStore = (
  client: MongoClient,
  db: Db,
  n: Names,
  id: IdGen,
  boundSession?: ClientSession,
): Store => {
  const runs = db.collection<RunDoc>(n.runs);
  const steps = db.collection<StepDoc>(n.steps);
  const signals = db.collection<SignalDoc>(n.signals);
  const crons = db.collection<CronDoc>(n.crons);
  const jobs = db.collection<JobDoc>(n.jobs);
  const timers = db.collection<TimerDoc>(n.timers);

  const inTx = async <T>(fn: (session: ClientSession) => Promise<T>): Promise<T> => {
    if (boundSession) return fn(boundSession); // already in the caller's transaction — mongo can't nest
    const session = client.startSession();
    try {
      return await session.withTransaction(fn);
    } finally {
      await session.endSession();
    }
  };

  const enqueue = (runId: string, opts: EnqueueOpts | undefined, session: ClientSession) =>
    enqueueJobs(jobs, runs, [{ runId, opts }], session);

  const insertSpawn = async (
    spec: RunSpec,
    runId: string,
    session: ClientSession,
  ): Promise<void> => {
    // An upsert, not insert-and-catch: a duplicate-key error inside a transaction aborts the whole
    // transaction, so first-writer-wins has to be expressed as a write that cannot fail.
    const { _id, ...doc } = buildRunDoc(spec, runId, new ObjectId());
    await runs.updateOne({ _id }, { $setOnInsert: doc }, { upsert: true, session });
  };

  const commitOutbox = async (fx: Outbox | undefined, session: ClientSession): Promise<void> => {
    if (!fx) return;
    const spawns = fx.spawn ?? [];
    for (const s of spawns) await insertSpawn(s.spec, s.runId, session);
    if (spawns.length) {
      await enqueueJobs(
        jobs,
        runs,
        spawns.map((s) => ({ runId: s.runId })),
        session,
      );
    }
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
      await signals.updateMany(
        { _id: { $in: [...fx.consumeSignals] } },
        { $set: { consumed: true } },
        { session },
      );
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
    const created: StartResult = { runId, created: true, status: "pending" };
    const run = buildRunDoc(spec, runId, new ObjectId());
    if (!spec.idempotencyKey) {
      await runs.insertOne(run, { session: boundSession });
      return created;
    }
    // An upsert, not insert-and-catch, for the reason on insertSpawn: under `inTx` a duplicate-key
    // error would abort the caller's transaction.
    const { name, version, idempotency_key, ...rest } = run;
    const existing = await runs.findOneAndUpdate(
      { name, version, idempotency_key },
      { $setOnInsert: rest },
      { upsert: true, returnDocument: "before", session: boundSession },
    );
    return existing ? { runId: existing._id, created: false, status: existing.status } : created;
  };

  const selectIds = async (
    q: Filter<RunDoc>,
    limit: number,
    session: ClientSession,
  ): Promise<string[]> => {
    const docs = await runs
      .find(q, { session })
      .sort({ ord: 1 })
      .limit(limit)
      .project<Pick<RunDoc, "_id">>({ _id: 1 })
      .toArray();
    return docs.map((r) => r._id);
  };

  const dropChildrenOfLiveParents = [
    {
      $lookup: {
        from: n.runs,
        let: { parent: "$parent_run_id" },
        pipeline: [
          {
            $match: {
              $expr: { $eq: ["$_id", "$$parent"] },
              status: { $nin: [...TERMINAL_STATUSES] },
            },
          },
          { $project: { _id: 1 } },
        ],
        as: "live_parent",
      },
    },
    { $match: { live_parent: { $size: 0 } } },
  ];

  const selectPurgeIds = async (
    sel: RunSelection,
    limit: number,
    session: ClientSession,
  ): Promise<string[]> => {
    const docs = await runs
      .aggregate<Pick<RunDoc, "_id">>(
        [
          { $match: selectionFilter(sel) },
          { $sort: { ord: 1 } },
          ...(sel.spareChildrenOfLiveParents ? dropChildrenOfLiveParents : []),
          { $limit: limit },
          { $project: { _id: 1 } },
        ],
        { session },
      )
      .toArray();
    return docs.map((r) => r._id);
  };

  const deleteRuns = async (filter: PurgeFilter, limit: number): Promise<number> => {
    const sel = purgeSelection(filter);
    const q = selectionFilter(sel);
    return inTx(async (session) => {
      const ids = await selectPurgeIds(sel, limit, session);
      if (ids.length === 0) return 0;
      // A run another writer moved off its terminal status since the read conflicts with this
      // delete and aborts the transaction into a retry, so every selected id is deleted.
      const res = await runs.deleteMany({ ...q, _id: { $in: ids } }, { session });
      await steps.deleteMany({ run_id: { $in: ids } }, { session });
      await signals.deleteMany({ run_id: { $in: ids } }, { session });
      await jobs.deleteMany({ _id: { $in: ids } }, { session });
      await timers.deleteMany({ _id: { $in: ids } }, { session });
      return res.deletedCount;
    });
  };

  return {
    startRun: startOne,

    async startManyRuns(specs) {
      if (!boundSession) return Promise.all(specs.map(startOne));
      // A session runs one operation at a time inside a transaction.
      const out: StartResult[] = [];
      for (const spec of specs) out.push(await startOne(spec));
      return out;
    },

    async loadRun(runId) {
      const run = await runs.findOne({ _id: runId });
      if (!run) return undefined;
      const [stepDocs, signalDocs] = await Promise.all([
        steps.find({ run_id: runId }).toArray(),
        signals
          .find({ run_id: runId, consumed: { $ne: true } })
          .sort({ ord: 1 })
          .toArray(),
      ]);
      const stepMap = new Map<string, StepOutcome>();
      for (const s of stepDocs) stepMap.set(s.cursor_key, mapStep(s));
      return {
        run: mapRun(run),
        steps: stepMap,
        signals: signalDocs.map(mapSignal),
      };
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
        payload: durable(payload),
        ord: new ObjectId(),
        ...(opts?.idempotencyKey !== undefined && {
          idem_key: opts.idempotencyKey,
        }),
      };
      return inTx(async (session) => {
        if (signalDoc.idem_key === undefined) {
          await signals.insertOne(signalDoc, { session });
        } else {
          const { run_id, idem_key, ...rest } = signalDoc;
          const prior = await signals.findOneAndUpdate(
            { run_id, idem_key },
            { $setOnInsert: rest },
            { upsert: true, returnDocument: "before", session },
          );
          if (prior) return { delivered: false };
        }
        await enqueue(runId, undefined, session); // wake the parked run atomically with delivery
        return { delivered: true };
      });
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
        ...(c.result !== undefined && { result: durable(c.result) }),
        ...(c.error !== undefined && { error: durableError(c.error) }),
        ...(c.call !== undefined && { call: c.call }),
      };
      return inTx(async (session) => {
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
        const { _id, ...rest } = stepDoc;
        const prior = await steps.findOneAndUpdate(
          { _id },
          { $setOnInsert: rest },
          { upsert: true, returnDocument: "before", session },
        );
        if (prior) return mapStep(prior); // first-writer-wins; skip the outbox
        await commitOutbox(fx, session);
        return mapStep(stepDoc);
      });
    },

    async suspendRun(runId, status: SuspendStatus, fx) {
      await inTx(async (session) => {
        const run = await runs.findOne({ _id: runId }, { session });
        if (!run) throw new Error(`suspendRun: run ${runId} not found`);
        if (isTerminal(run.status)) return; // already terminal — nothing to park
        const set = resetsAttempts(status) ? { status, attempts: 0 } : { status };
        await runs.updateOne({ _id: runId }, { $set: set }, { session });
        await commitOutbox(fx, session);
      });
    },

    markTerminal(runId, outcome, fx) {
      return inTx(async (session) => {
        const run = await runs.findOne({ _id: runId }, { session });
        if (!run) throw new Error(`markTerminal: run ${runId} not found`);
        if (isTerminal(run.status)) return false;
        const output = outcome.status === "done" ? durable(outcome.output) : undefined;
        const error =
          outcome.status === "done" || outcome.error === undefined
            ? undefined
            : durableError(outcome.error);
        await runs.updateOne(
          { _id: runId },
          {
            $set: {
              status: outcome.status,
              ...(output !== undefined && { output }),
              ...(error !== undefined && { error }),
            },
            $unset: {
              ...(output === undefined && { output: "" }),
              ...(error === undefined && { error: "" }),
            },
          },
          { session },
        );
        await commitOutbox(fx, session);
        return true;
      });
    },

    async listRuns(filter, page) {
      const q: Filter<RunDoc> = {
        ...selectionFilter(listSelection(filter)),
        ...(page.cursor && { ord: { $lt: new ObjectId(page.cursor) } }),
      };
      const docs = await runs.find(q).sort({ ord: -1 }).limit(page.limit).toArray();
      const last = docs[docs.length - 1];
      const cursor = docs.length === page.limit && last ? last.ord.toHexString() : undefined;
      return { runs: docs.map(mapRun), cursor };
    },

    async cancelRuns(filter, limit) {
      const q = selectionFilter(cancelSelection(filter));
      return inTx(async (session) => {
        const ids = await selectIds(q, limit, session);
        if (ids.length === 0) return 0;
        const res = await runs.updateMany(
          { ...q, _id: { $in: ids } },
          { $set: { status: "canceled" } },
          { session },
        );
        await timers.deleteMany({ _id: { $in: ids } }, { session });
        return res.modifiedCount;
      });
    },

    async retryRuns(filter, limit) {
      const q = selectionFilter(retrySelection(filter));
      return inTx(async (session) => {
        const ids = await selectIds(q, limit, session);
        if (ids.length === 0) return 0;
        const res = await runs.updateMany(
          { ...q, _id: { $in: ids } },
          { $set: { status: "pending", attempts: 0 }, $unset: { error: "" } },
          { session },
        );
        // A run another writer moved off `failed` since the read conflicts with this update and
        // aborts the transaction into a retry, so every selected id was flipped.
        await enqueueJobs(
          jobs,
          runs,
          ids.map((runId) => ({ runId })),
          session,
        );
        return res.modifiedCount;
      });
    },

    async childrenOf(runId) {
      const docs = await runs.find({ parent_run_id: runId }).toArray();
      return docs.map(mapRun);
    },

    async runStats() {
      const stats = zeroRunStats();
      const grouped = await runs
        .aggregate<{
          _id: keyof typeof stats;
          count: number;
        }>([{ $group: { _id: "$status", count: { $sum: 1 } } }])
        .toArray();
      for (const g of grouped) stats[g._id] = g.count;
      return stats;
    },

    deleteRuns,

    async orphanedRuns(limit) {
      const runDocs = await runs.find({}).sort({ ord: 1 }).toArray();
      const ids = runDocs.map((r) => r._id);
      const [jobIds, timerIds] = await Promise.all([
        jobs.distinct("_id", { _id: { $in: ids } }),
        timers.distinct("_id", { _id: { $in: ids } }),
      ]);
      const hasJob = new Set(jobIds);
      const hasTimer = new Set(timerIds);
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
          { $set: { status: "pending", attempts: 0 }, $unset: { error: "" } },
          { session },
        );
        const retried = res.modifiedCount === 1;
        if (retried) await enqueue(runId, undefined, session);
        return { retried };
      });
    },

    async upsertCron(spec) {
      // A pipeline update so the comparison is atomic: inside one $set every expression still sees
      // the pre-update doc, so `$schedule` is the OLD schedule.
      const at = spec.nextRunAt.getTime();
      await crons.updateOne(
        { _id: spec.name },
        [
          {
            $set: {
              schedule: spec.schedule,
              flow_name: spec.flowName,
              flow_version: spec.flowVersion,
              input: durable(spec.input),
              overlap: spec.overlap,
              next_run_at: {
                $cond: [
                  { $eq: ["$schedule", spec.schedule] },
                  { $ifNull: ["$next_run_at", at] },
                  at,
                ],
              },
              last_run_at: { $ifNull: ["$last_run_at", null] },
            },
          },
        ],
        { upsert: true },
      );
    },

    async nextCronAt() {
      const [doc] = await crons.find({}).sort({ next_run_at: 1 }).limit(1).toArray();
      return doc ? mapCron(doc).nextRunAt : null;
    },

    async dueCrons(now, limit) {
      const docs = await crons
        .find({ next_run_at: { $lte: now.getTime() } })
        .sort({ next_run_at: 1 })
        .limit(limit)
        .toArray();
      return docs.map(mapCron);
    },

    async listCrons() {
      const docs = await crons.find({}).sort({ _id: 1 }).toArray();
      return docs.map(mapCron);
    },

    async removeCron(name) {
      const res = await crons.deleteOne({ _id: name });
      return res.deletedCount === 1;
    },

    async dueCronCount(now, names) {
      const q: Filter<CronDoc> = {
        next_run_at: { $lte: now.getTime() },
        ...(names && { flow_name: { $in: [...names] } }),
      };
      return crons.countDocuments(q);
    },

    async advanceCron(name, expectedNextRunAt, nextRunAt, lastRunAt) {
      const res = await crons.updateOne(
        { _id: name, next_run_at: expectedNextRunAt.getTime() },
        {
          $set: {
            next_run_at: nextRunAt.getTime(),
            last_run_at: lastRunAt.getTime(),
          },
        },
      );
      return res.modifiedCount === 1;
    },
  };
};
