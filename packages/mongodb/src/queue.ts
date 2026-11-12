import {
  type ClaimOpts,
  type EnqueueOpts,
  type EnqueueRequest,
  type IdGen,
  type Lease,
  type Queue,
  distinctEnqueues,
  enqueueFields,
  queueDepthOf,
} from "@iterativeflow/core/backend";
import type { ClientSession, Collection, Db, Filter, UpdateFilter } from "mongodb";
import type { RunDoc } from "#codec";
import type { Names } from "#collections";

/** @internal */
export interface JobDoc {
  _id: string;
  run_at: number;
  priority: number;
  version: number;
  lease_token?: string;
  lease_expires?: number;
}

const unleasedAt = (at: number): Filter<JobDoc> => ({
  $or: [{ lease_expires: { $exists: false } }, { lease_expires: { $lte: at } }],
});

/** @internal */
export const claimableJobs = (at: number): Filter<JobDoc> => ({
  run_at: { $lte: at },
  ...unleasedAt(at),
});

const jobUpdate = (
  opts: EnqueueOpts | undefined,
  runPriority: number | undefined,
): UpdateFilter<JobDoc> => {
  const { runAtMs, priority } = enqueueFields(opts, runPriority);
  return { $set: { run_at: runAtMs, priority }, $inc: { version: 1 } };
};

/** @internal */
export const enqueueJobs = async (
  jobs: Collection<JobDoc>,
  runs: Collection<RunDoc>,
  requests: readonly EnqueueRequest[],
  session: ClientSession | undefined,
): Promise<void> => {
  // An unordered bulk would race two upserts of the same _id into a duplicate-key error.
  const latest = distinctEnqueues(requests);
  if (latest.length === 0) return;
  const unprioritized = latest.filter(([, opts]) => opts?.priority === undefined).map(([id]) => id);
  const stored = unprioritized.length
    ? await runs
        .find({ _id: { $in: unprioritized } }, { session })
        .project<{ _id: string; priority?: number }>({ priority: 1 })
        .toArray()
    : [];
  const runPriority = new Map(stored.map((r) => [r._id, r.priority]));
  await jobs.bulkWrite(
    latest.map(([runId, opts]) => ({
      updateOne: {
        filter: { _id: runId },
        update: jobUpdate(opts, runPriority.get(runId)),
        upsert: true,
      },
    })),
    { ordered: false, session },
  );
};

/** @internal */
export const createMongoQueue = (
  db: Db,
  n: Names,
  id: IdGen,
  boundSession?: ClientSession,
): Queue => {
  const jobs = db.collection<JobDoc>(n.jobs);
  const runs = db.collection<RunDoc>(n.runs);
  const ms = (now?: Date): number => (now ?? new Date()).getTime();

  return {
    async enqueue(runId, opts) {
      await enqueueJobs(jobs, runs, [{ runId, opts }], boundSession);
    },

    async enqueueMany(requests) {
      await enqueueJobs(jobs, runs, requests, boundSession);
    },

    async claim({ limit, leaseMs, now, names }: ClaimOpts) {
      if (names?.length === 0) return [];
      const t = ms(now);
      // The name filter runs before the limit: limiting first lets other shards' higher-ranked
      // jobs fill every page and starve this one. A run-less job passes (see ClaimOpts.names).
      const shard = names
        ? [
            {
              $lookup: {
                from: n.runs,
                localField: "_id",
                foreignField: "_id",
                pipeline: [{ $project: { name: 1 } }],
                as: "run",
              },
            },
            { $match: { $or: [{ run: { $size: 0 } }, { "run.name": { $in: [...names] } }] } },
          ]
        : [];
      const candidates = await jobs
        .aggregate<Pick<JobDoc, "_id">>([
          { $match: claimableJobs(t) },
          { $sort: { priority: 1, run_at: 1 } },
          ...shard,
          { $limit: limit },
          { $project: { _id: 1 } },
        ])
        .toArray();
      const leases: Lease[] = [];
      for (const cand of candidates) {
        const token = `${id()}:${cand._id}`;
        const expires = t + leaseMs;
        const won = await jobs.findOneAndUpdate(
          { _id: cand._id, ...unleasedAt(t) },
          { $set: { lease_token: token, lease_expires: expires } },
          { returnDocument: "before" },
        );
        if (won) {
          leases.push({
            runId: cand._id,
            token,
            expiresAt: new Date(expires),
            // `won` is the doc AS OF the atomic lease write; a wake bumping version between the
            // candidate `find` and here is captured here, not by the stale `cand`.
            version: won.version,
          });
        }
      }
      return leases;
    },

    async heartbeat(lease: Lease, { leaseMs, now }) {
      const t = ms(now);
      const expires = t + leaseMs;
      const held = await jobs.findOneAndUpdate(
        { _id: lease.runId, lease_token: lease.token, lease_expires: { $gt: t } },
        { $set: { lease_expires: expires } },
      );
      if (!held) throw new Error(`heartbeat: lease for ${lease.runId} is no longer held`);
      return { ...lease, expiresAt: new Date(expires) };
    },

    async ack(lease: Lease, opts) {
      const t = ms(opts?.now);
      const held = { lease_token: lease.token, lease_expires: { $gt: t } };
      const done = await jobs.deleteOne({ _id: lease.runId, ...held, version: lease.version });
      if (done.deletedCount === 0) {
        await jobs.updateOne(
          { _id: lease.runId, ...held, version: { $ne: lease.version } },
          { $unset: { lease_token: "", lease_expires: "" }, $set: { run_at: 0 } },
        );
      }
    },

    async depth(now, names) {
      if (names?.length === 0) return queueDepthOf([], ms(now));
      let all = await jobs
        .find({})
        .project<{ _id: string; run_at: number; lease_expires?: number }>({
          run_at: 1,
          lease_expires: 1,
        })
        .toArray();
      if (names) {
        // a run-less job is unownable, so it passes every name filter (see Queue.claim)
        const rejected = await runs
          .find({ _id: { $in: all.map((j) => j._id) }, name: { $nin: [...names] } })
          .project({ _id: 1 })
          .toArray();
        const dropped = new Set(rejected.map((r) => r._id));
        all = all.filter((j) => !dropped.has(j._id));
      }
      return queueDepthOf(
        all.map((j) => ({ runAt: j.run_at, leaseExpires: j.lease_expires })),
        ms(now),
      );
    },
  };
};
