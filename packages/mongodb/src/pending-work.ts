import type { Document } from "mongodb";
import { names } from "#collections";
import { claimableJobs } from "#queue";
import { dueTimers } from "#timer";

/**
 * Aggregation pipeline that returns the autoscaling backlog — claimable jobs + due timers + due crons —
 * as `[{ pendingWork: N }]` (an empty result means `0`). Run it on the `jobs` collection (prefixed, if
 * `opts.prefix` is set). MongoDB has no stored functions, and KEDA's mongodb scaler counts one
 * collection with a filter document rather than running an aggregation — so this `$unionWith` pipeline
 * is for a dashboard, or a scaler that can run an aggregation, not for that scaler directly. It's the
 * whole backlog; for a per-shard count use `engine.pendingWork(names)` / the dashboard's
 * `/api/metrics`.
 */
export const pendingWorkPipeline = (
  now: Date | number,
  opts: { prefix?: string } = {},
): Document[] => {
  const at = now instanceof Date ? now.getTime() : now;
  const n = names(opts.prefix ?? "");
  return [
    { $match: claimableJobs(at) },
    { $project: { _id: 1 } },
    {
      $unionWith: {
        coll: n.timers,
        pipeline: [{ $match: dueTimers(at) }, { $project: { _id: 1 } }],
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
