import type { Pool } from "pg";
import type { Dispatcher, DispatcherStartOpts } from "../../engine/scheduler";
import type { Logger } from "../../engine/types";
import type { WorkflowDb } from "../../storage/db";
import { type GraphileWorker, startGraphileWorker } from "./index";

/** Process/runner config for {@link createGraphileDispatcher}, owned by the engine. */
export interface GraphileDispatcherOpts {
  pool: Pool;
  db: WorkflowDb;
  logger: Logger;
  schema?: string;
  concurrency?: number;
  pollInterval?: number;
}

/**
 * Default {@link Dispatcher}: a resident graphile-worker that polls the queue
 * and calls `handleRun`. The engine's `listen()`/`stop()`/`health()` route
 * through this seam instead of calling the worker directly.
 *
 * @internal
 */
export const createGraphileDispatcher = (cfg: GraphileDispatcherOpts): Dispatcher => {
  let worker: GraphileWorker | null = null;
  return {
    async start(opts: DispatcherStartOpts) {
      worker = await startGraphileWorker({
        pool: cfg.pool,
        db: cfg.db,
        schema: cfg.schema,
        concurrency: cfg.concurrency,
        pollInterval: cfg.pollInterval,
        logger: cfg.logger,
        crons: opts.crons,
        flows: opts.flows,
        runCron: opts.runCron,
        runWorkflow: opts.handleRun,
      });
    },
    async stop() {
      if (worker) {
        await worker.stop();
        worker = null;
      }
    },
    running: () => worker !== null,
  };
};
