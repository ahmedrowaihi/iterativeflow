import type { Document } from "mongodb";
import { names } from "#collections";

/**
 * Aggregation pipeline that returns the autoscaling backlog — claimable jobs + due timers + due crons —
 * as `[{ pendingWork: N }]` (an empty result means `0`). Run it on the `jobs` collection. MongoDB has
 * no stored functions, and KEDA's mongodb scaler counts one collection, so this `$unionWith` pipeline is
 * how a mongo-side metric spans all three. It's the whole backlog; for a per-shard count use
 * `engine.pendingWork(names)` / the dashboard's `/api/metrics`.
 */
export const pendingWorkPipeline = (
  now: Date | number,
  opts: { prefix?: string } = {},
): Document[] => {
  const at = typeof now === "number" ? now : now.getTime();
  const n = names(opts.prefix ?? "");
  return [
    {
      $match: {
        run_at: { $lte: at },
        $or: [{ lease_expires: null }, { lease_expires: { $lte: at } }],
      },
    },
    { $project: { _id: 1 } },
    {
      $unionWith: {
        coll: n.timers,
        pipeline: [{ $match: { fire_at: { $lte: at } } }, { $project: { _id: 1 } }],
      },
    },
    {
      $unionWith: {
        coll: n.crons,
        pipeline: [{ $match: { next_run_at: { $lte: at } } }, { $project: { _id: 1 } }],
      },
    },
    { $count: "pendingWork" },
  ];
};
