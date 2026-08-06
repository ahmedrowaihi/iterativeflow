import type { ClaimOpts, IdGen, Lease, Queue } from "@iterativeflow/core/backend";
import { queueDepthOf } from "@iterativeflow/core/backend";
import type { ClientSession, Db } from "mongodb";
import type { Names } from "#collections";

interface JobDoc {
  _id: string;
  run_at: number;
  priority: number;
  version: number;
  lease_token?: string;
  lease_expires?: number;
}

/** @internal */
export const createMongoQueue = (
  db: Db,
  n: Names,
  id: IdGen,
  boundSession?: ClientSession,
): Queue => {
  const jobs = db.collection<JobDoc>(n.jobs);
  const runs = db.collection<{ _id: string; name: string }>(n.runs);
  const ms = (now?: Date): number => (now ?? new Date()).getTime();

  return {
    async enqueue(runId, opts) {
      await jobs.updateOne(
        { _id: runId },
        {
          $set: { run_at: opts?.runAt ? opts.runAt.getTime() : 0, priority: opts?.priority ?? 0 },
          $inc: { version: 1 },
        },
        { upsert: true, session: boundSession },
      );
    },

    async claim({ limit, leaseMs, now, names }: ClaimOpts) {
      const t = ms(now);
      const unleased = [{ lease_expires: { $exists: false } }, { lease_expires: { $lte: t } }];
      let candidates = await jobs
        .find({ run_at: { $lte: t }, $or: unleased })
        .sort({ priority: 1, run_at: 1 })
        .limit(limit)
        .toArray();
      if (names) {
        const wanted = new Set(names);
        const present = await runs
          .find({ _id: { $in: candidates.map((c) => c._id) } })
          .project<{ _id: string; name: string }>({ name: 1 })
          .toArray();
        const nameById = new Map(present.map((r) => [r._id, r.name]));
        candidates = candidates.filter(
          (c) => !nameById.has(c._id) || wanted.has(nameById.get(c._id) ?? ""),
        );
      }
      const leases: Lease[] = [];
      for (const cand of candidates) {
        const token = `${id()}:${cand._id}`;
        const expires = t + leaseMs;
        const won = await jobs.findOneAndUpdate(
          { _id: cand._id, $or: unleased },
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
      let all = await jobs
        .find({})
        .project<{ _id: string; run_at: number; lease_expires?: number }>({
          run_at: 1,
          lease_expires: 1,
        })
        .toArray();
      if (names) {
        const allowed = await runs
          .find({ _id: { $in: all.map((j) => j._id) }, name: { $in: [...names] } })
          .project({ _id: 1 })
          .toArray();
        const allowedIds = new Set(allowed.map((r) => r._id));
        all = all.filter((j) => allowedIds.has(j._id));
      }
      return queueDepthOf(
        all.map((j) => ({ runAt: j.run_at, leaseExpires: j.lease_expires })),
        ms(now),
      );
    },
  };
};
