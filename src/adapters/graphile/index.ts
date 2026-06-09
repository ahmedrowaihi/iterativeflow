import { sql } from "drizzle-orm";
import { ts } from "../../util/sql-params";
import {
  parseCronItems,
  run,
  type Runner,
  type RunnerOptions,
  type TaskList,
} from "graphile-worker";
import type { Pool } from "pg";
import type { CronSpec, Logger } from "../../engine/types";
import type { WorkflowDb } from "../../storage/db";
import type { TxEnqueue } from "../../storage/drizzle";
import { CRON_TASK_PREFIX, buildCronHandler, reapOrphanedCronJobs } from "./cron";

export const FLOW_TASK = "flow:run";

export const createGraphileTxEnqueue = (workerSchema: string = "graphile_worker"): TxEnqueue => {
  const schema = sql.identifier(workerSchema);
  return async (tx, runId, opts) => {
    const jobKey = `flow:${runId}`;
    const runAt = opts?.runAt ? ts(opts.runAt) : sql`NULL`;
    await tx.execute(sql`
      SELECT ${schema}.add_job(
        identifier => ${FLOW_TASK},
        payload => ${sql`json_build_object('runId', ${runId}::text)`},
        run_at => ${runAt},
        priority => ${opts?.priority ?? null},
        job_key => ${jobKey},
        job_key_mode => ${"replace"}
      )
    `);
  };
};

export interface GraphileWorkerOpts {
  pool: Pool;
  db: WorkflowDb;
  schema?: string;
  concurrency?: number;
  pollInterval?: number;
  logger: Logger;
  crons: CronSpec[];
  runWorkflow: (runId: string) => Promise<void>;
  runCron: (name: string, fn: () => Promise<void>) => Promise<void>;
}

export interface GraphileWorker {
  runner: Runner;
  stop(): Promise<void>;
}

export const validateCron = (pattern: string): void => {
  const fields = pattern.trim().split(/\s+/);
  if (fields.length !== 5) {
    throw new Error(`Invalid cron pattern "${pattern}": expected 5 fields, got ${fields.length}`);
  }
};

export const startGraphileWorker = async (opt: GraphileWorkerOpts): Promise<GraphileWorker> => {
  for (const c of opt.crons) validateCron(c.schedule);

  const parsedCronItems = parseCronItems(
    opt.crons.map((c) => ({
      task: `${CRON_TASK_PREFIX}${c.name}`,
      match: buildMatchPattern(c.schedule),
      identifier: c.name,
      ...(c.backfillPeriod ? { options: { backfillPeriod: c.backfillPeriod } } : {}),
    })),
  );

  const cronTasks: TaskList = Object.fromEntries(
    opt.crons.map((c) => [
      `${CRON_TASK_PREFIX}${c.name}`,
      buildCronHandler(c, opt.logger, opt.runCron, opt.pool),
    ]),
  );

  const options: RunnerOptions = {
    pgPool: opt.pool,
    schema: opt.schema ?? "graphile_worker",
    concurrency: opt.concurrency ?? 5,
    pollInterval: opt.pollInterval ?? 1000,
    noHandleSignals: true,
    parsedCronItems,
    taskList: {
      [FLOW_TASK]: async (payload, helpers) => {
        const { runId } = payload as { runId?: string };
        if (!runId) {
          helpers.logger.warn("flow task missing runId");
          return;
        }
        await opt.runWorkflow(runId);
      },
      ...cronTasks,
    },
  };

  const runner = await run(options);
  await reapOrphanedCronJobs(opt.db, options.schema ?? "graphile_worker", opt.crons, opt.logger);
  opt.logger.info("flow.worker.started", {
    schema: options.schema,
    concurrency: options.concurrency,
    crons: opt.crons.map((c) => `${c.name}@${c.schedule}`),
  });

  return {
    runner,
    async stop() {
      await runner.stop();
    },
  };
};

const buildMatchPattern = (schedule: string): string => schedule;
