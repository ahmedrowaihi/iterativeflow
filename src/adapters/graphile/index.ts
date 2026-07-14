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

/**
 * graphile task identifier for a flow. Per-flow (not a shared `"flow:run"`) so
 * graphile routes a run only to workers that registered that exact
 * `name@version` — a worker physically cannot claim a flow it didn't register.
 *
 * @internal
 */
export const flowTaskId = (name: string, version: number): string => `flow:run:${name}@${version}`;

export const createGraphileTxEnqueue = (workerSchema: string = "graphile_worker"): TxEnqueue => {
  const schema = sql.identifier(workerSchema);
  const enqueue: TxEnqueue = async (tx, job, opts) => {
    const jobKey = `flow:${job.runId}`;
    const runAt = opts?.runAt ? ts(opts.runAt) : sql`NULL`;
    await tx.execute(sql`
      SELECT ${schema}.add_job(
        identifier => ${flowTaskId(job.name, job.version)},
        payload => ${sql`json_build_object('runId', ${job.runId}::text)`},
        run_at => ${runAt},
        priority => ${opts?.priority ?? null},
        job_key => ${jobKey},
        job_key_mode => ${"replace"}
      )
    `);
  };

  // add_job (not the newer bulk add_jobs) so the fragment stays valid across graphile-worker versions.
  enqueue.many = async (tx, jobs) => {
    if (jobs.length === 0) return;
    const rows = jobs.map(
      ({ job, opts }) =>
        sql`(${flowTaskId(job.name, job.version)}, ${job.runId}, ${
          opts?.runAt ? opts.runAt.toISOString() : null
        }::timestamptz, ${opts?.priority ?? null}::int, ${`flow:${job.runId}`})`,
    );
    await tx.execute(sql`
      SELECT ${schema}.add_job(
        identifier => v.identifier,
        payload => json_build_object('runId', v.run_id),
        run_at => v.run_at,
        priority => v.priority,
        job_key => v.job_key,
        job_key_mode => ${"replace"}
      )
      FROM (VALUES ${sql.join(rows, sql`, `)}) AS v(identifier, run_id, run_at, priority, job_key)
    `);
  };

  return enqueue;
};

export interface GraphileWorkerOpts {
  pool: Pool;
  db: WorkflowDb;
  schema?: string;
  concurrency?: number;
  pollInterval?: number;
  logger: Logger;
  crons: CronSpec[];
  flows: ReadonlyArray<{ name: string; version: number }>;
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

  const flowTasks: TaskList = Object.fromEntries(
    opt.flows.map(({ name, version }) => [
      flowTaskId(name, version),
      async (payload: unknown, helpers) => {
        const { runId } = payload as { runId?: string };
        if (!runId) {
          helpers.logger.warn("flow task missing runId");
          return;
        }
        await opt.runWorkflow(runId);
      },
    ]),
  );

  const options: RunnerOptions = {
    pgPool: opt.pool,
    schema: opt.schema ?? "graphile_worker",
    concurrency: opt.concurrency ?? 5,
    pollInterval: opt.pollInterval ?? 1000,
    noHandleSignals: true,
    parsedCronItems,
    taskList: { ...flowTasks, ...cronTasks },
  };

  const runner = await run(options);
  await reapOrphanedCronJobs(opt.db, options.schema ?? "graphile_worker", opt.crons, opt.logger);
  opt.logger.info("flow.worker.started", {
    schema: options.schema,
    concurrency: options.concurrency,
    flows: opt.flows.map(({ name, version }) => `${name}@${version}`),
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

export { createGraphileDispatcher, type GraphileDispatcherOpts } from "./dispatcher";
