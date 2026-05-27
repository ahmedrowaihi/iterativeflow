import { sql } from "drizzle-orm";
import {
  parseCronItems,
  run,
  type Runner,
  type RunnerOptions,
  type TaskList,
} from "graphile-worker";
import type { Pool } from "pg";
import type { CronSpec, Logger } from "../engine/types";
import type { TxEnqueue } from "../storage/drizzle";

export const WORKFLOW_TASK = "workflow:run";
const CRON_TASK_PREFIX = "cron:";

export const createGraphileTxEnqueue = (workerSchema: string = "graphile_worker"): TxEnqueue => {
  const schema = sql.identifier(workerSchema);
  return async (tx, runId, opts) => {
    const jobKey = `workflow:${runId}`;
    await tx.execute(sql`
      SELECT ${schema}.add_job(
        identifier => ${WORKFLOW_TASK},
        payload => ${sql`json_build_object('runId', ${runId}::text)`},
        run_at => ${opts?.runAt ?? null},
        priority => ${opts?.priority ?? null},
        job_key => ${jobKey},
        job_key_mode => ${"replace"}
      )
    `);
  };
};

export interface GraphileWorkerOpts {
  pool: Pool;
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

const buildCronHandler =
  (spec: CronSpec, logger: Logger, runCron: GraphileWorkerOpts["runCron"]) => async () => {
    try {
      await runCron(spec.name, async () => {
        await spec.run();
      });
    } catch (err) {
      logger.error(err instanceof Error ? err : new Error(String(err)), {
        event: "workflow.cron.failed",
        cron: spec.name,
      });
    }
  };

export const startGraphileWorker = async (opt: GraphileWorkerOpts): Promise<GraphileWorker> => {
  const parsedCronItems = parseCronItems(
    opt.crons.map((c) => ({
      task: `${CRON_TASK_PREFIX}${c.name}`,
      match: c.schedule,
      identifier: c.name,
      ...(c.backfillPeriod ? { options: { backfillPeriod: c.backfillPeriod } } : {}),
    })),
  );

  const cronTasks: TaskList = Object.fromEntries(
    opt.crons.map((c) => [
      `${CRON_TASK_PREFIX}${c.name}`,
      buildCronHandler(c, opt.logger, opt.runCron),
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
      [WORKFLOW_TASK]: async (payload, helpers) => {
        const { runId } = payload as { runId?: string };
        if (!runId) {
          helpers.logger.warn("workflow task missing runId");
          return;
        }
        await opt.runWorkflow(runId);
      },
      ...cronTasks,
    },
  };

  const runner = await run(options);
  opt.logger.info("workflow.worker.started", {
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
