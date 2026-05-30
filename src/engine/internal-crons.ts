import { type Duration, toMs } from "../util/duration";
import type { CronSpec, MetricsRecorder, Storage } from "./types";

/** @internal */
export const RECONCILE_CRON_NAME = "__iterativeflow_reconcile";
/** @internal */
export const RETENTION_CRON_NAME = "__iterativeflow_retention";

/** @internal */
export interface ReconcilerCronOpts {
  storage: Storage;
  metrics: MetricsRecorder;
  graceMs: number;
  stuckMs: number;
}

/**
 * Build the orphan-reconciler cron — runs once per minute, re-enqueues runs
 * whose status looks stuck.
 *
 * @internal
 */
export const buildReconcilerCron = ({
  storage,
  metrics,
  graceMs,
  stuckMs,
}: ReconcilerCronOpts): CronSpec => ({
  name: RECONCILE_CRON_NAME,
  schedule: "* * * * *",
  run: async () => {
    const reEnqueued = await storage.reenqueueOrphans({
      olderThan: new Date(Date.now() - graceMs),
      runningStuckOlderThan: new Date(Date.now() - stuckMs),
    });
    metrics.reconcilerSweep?.({ scanned: reEnqueued, reEnqueued });
  },
});

/** @internal */
export interface RetentionCronOpts {
  storage: Storage;
  retention: {
    runsOlderThan?: Duration;
    eventsOlderThan?: Duration;
    schedule?: string;
    batchSize?: number;
  };
}

/**
 * Build the retention cron from `EngineOpts.retention`. Returns the cron
 * spec; the caller wires it into the worker.
 *
 * @internal
 */
export const buildRetentionCron = ({ storage, retention }: RetentionCronOpts): CronSpec => {
  const batchSize = retention.batchSize ?? 1000;
  return {
    name: RETENTION_CRON_NAME,
    schedule: retention.schedule ?? "0 * * * *",
    run: async () => {
      if (retention.eventsOlderThan !== undefined) {
        await storage.pruneEvents({
          olderThan: new Date(Date.now() - toMs(retention.eventsOlderThan)),
          batchSize,
        });
      }
      if (retention.runsOlderThan !== undefined) {
        await storage.pruneRuns({
          olderThan: new Date(Date.now() - toMs(retention.runsOlderThan)),
          batchSize,
        });
      }
    },
  };
};
