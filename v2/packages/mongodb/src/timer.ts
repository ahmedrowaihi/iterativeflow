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

    async cancel(runId) {
      await timers.deleteOne({ _id: runId });
    },
  };
};
