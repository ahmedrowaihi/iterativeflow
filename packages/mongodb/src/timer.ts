import type { Timer, TimerDueOpts } from "@iterativeflow/core/backend";
import type { Db } from "mongodb";
import type { Names } from "#collections";

interface TimerDoc {
  _id: string;
  fire_at: number;
}

/** @internal */
export const createMongoTimer = (db: Db, n: Names): Timer => {
  const timers = db.collection<TimerDoc>(n.timers);
  const runs = db.collection<{ _id: string; name: string }>(n.runs);

  return {
    async schedule(runId, fireAt) {
      await timers.updateOne(
        { _id: runId },
        { $set: { fire_at: fireAt.getTime() } },
        { upsert: true },
      );
    },

    async dueBatch({ now, limit }: TimerDueOpts) {
      const t = (now ?? new Date()).getTime();
      const due = await timers
        .find({ fire_at: { $lte: t } })
        .sort({ fire_at: 1 })
        .limit(limit)
        .toArray();
      const ids = due.map((d) => d._id);
      if (ids.length) await timers.deleteMany({ _id: { $in: ids } });
      return ids;
    },

    async dueCount(now, names) {
      const t = now.getTime();
      if (!names) return timers.countDocuments({ fire_at: { $lte: t } });
      const due = await timers
        .find({ fire_at: { $lte: t } })
        .project({ _id: 1 })
        .toArray();
      if (due.length === 0) return 0;
      const allowed = await runs
        .find({ _id: { $in: due.map((d) => d._id) }, name: { $in: [...names] } })
        .project({ _id: 1 })
        .toArray();
      return allowed.length;
    },

    async cancel(runId) {
      await timers.deleteOne({ _id: runId });
    },

    async nextDueAt(now) {
      const next = await timers.findOne(
        { fire_at: { $gt: now.getTime() } },
        { sort: { fire_at: 1 } },
      );
      return next ? new Date(next.fire_at) : null;
    },
  };
};
