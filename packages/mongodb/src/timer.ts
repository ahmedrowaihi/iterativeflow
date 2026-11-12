import type { Timer, TimerDueOpts } from "@iterativeflow/core/backend";
import type { ClientSession, Db, Filter } from "mongodb";
import type { TimerDoc } from "#codec";
import type { Names } from "#collections";

/** @internal */
export const dueTimers = (at: number): Filter<TimerDoc> => ({ fire_at: { $lte: at } });

/** @internal */
export const createMongoTimer = (db: Db, n: Names, session: ClientSession | undefined): Timer => {
  const timers = db.collection<TimerDoc>(n.timers);
  const runs = db.collection<{ _id: string; name: string }>(n.runs);

  return {
    async schedule(runId, fireAt) {
      await timers.updateOne(
        { _id: runId },
        { $set: { fire_at: fireAt.getTime() } },
        { upsert: true, session },
      );
    },

    async dueBatch({ now, limit }: TimerDueOpts) {
      const t = (now ?? new Date()).getTime();
      const due = await timers.find(dueTimers(t)).sort({ fire_at: 1 }).limit(limit).toArray();
      const fired: string[] = [];
      for (const d of due) {
        // Concurrent drainers read the same candidates; only the delete that lands fires the timer.
        const res = await timers.deleteOne({ _id: d._id, ...dueTimers(t) });
        if (res.deletedCount === 1) fired.push(d._id);
      }
      return fired;
    },

    async dueCount(now, names) {
      const t = now.getTime();
      if (names?.length === 0) return 0;
      if (!names) return timers.countDocuments(dueTimers(t));
      const due = await timers.find(dueTimers(t)).project({ _id: 1 }).toArray();
      if (due.length === 0) return 0;
      // a run-less timer is unownable, so it passes every name filter (see Queue.claim)
      const rejected = await runs
        .find({ _id: { $in: due.map((d) => d._id) }, name: { $nin: [...names] } })
        .project({ _id: 1 })
        .toArray();
      return due.length - rejected.length;
    },

    async cancel(runId) {
      await timers.deleteOne({ _id: runId }, { session });
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
