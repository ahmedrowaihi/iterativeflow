import { sql } from "drizzle-orm";
import {
  parseCronItems,
  run,
  type Runner,
  type RunnerOptions,
  type TaskList,
} from "graphile-worker";
import type { Pool } from "pg";
import type { CronSpec, Logger } from "../../engine/types";
import type { TxEnqueue } from "../../storage/drizzle";

export const FLOW_TASK = "flow:run";
const CRON_TASK_PREFIX = "cron:";

export const createGraphileTxEnqueue = (workerSchema: string = "graphile_worker"): TxEnqueue => {
  const schema = sql.identifier(workerSchema);
  return async (tx, runId, opts) => {
    const jobKey = `flow:${runId}`;
    await tx.execute(sql`
      SELECT ${schema}.add_job(
        identifier => ${FLOW_TASK},
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

export const validateCron = (pattern: string): void => {
  const fields = pattern.trim().split(/\s+/);
  if (fields.length !== 5) {
    throw new Error(`Invalid cron pattern "${pattern}": expected 5 fields, got ${fields.length}`);
  }
};

const wrapBody =
  (spec: CronSpec): (() => Promise<void>) =>
  async () => {
    // Apply jitter so concurrent engine instances don't fire simultaneously.
    if (spec.jitterMs && spec.jitterMs > 0) {
      await new Promise((r) => setTimeout(r, Math.floor(spec.jitterMs! * Math.random())));
    }
    await spec.run();
  };

const buildCronHandler =
  (spec: CronSpec, logger: Logger, runCron: GraphileWorkerOpts["runCron"], pool: Pool) =>
  async () => {
    try {
      if (spec.overlap === "allow") {
        await runCron(spec.name, () => wrapBody(spec)());
        return;
      }
      const client = await pool.connect();
      try {
        const lockKey = hashCronName(spec.name);
        const got = await client.query<{ ok: boolean }>("SELECT pg_try_advisory_lock($1) AS ok", [
          lockKey,
        ]);
        if (!got.rows[0]?.ok) {
          logger.debug("flow.cron.skipped_overlap", { cron: spec.name });
          return;
        }
        try {
          await runCron(spec.name, () => wrapBody(spec)());
        } finally {
          await client.query("SELECT pg_advisory_unlock($1)", [lockKey]).catch(() => undefined);
        }
      } finally {
        client.release();
      }
    } catch (err) {
      logger.error(err instanceof Error ? err : new Error(String(err)), {
        event: "flow.cron.failed",
        cron: spec.name,
      });
      throw err;
    }
  };

const hashCronName = (name: string): number => {
  // Stable, signed 32-bit hash safe for pg_advisory_lock(bigint).
  let h = 0;
  for (let i = 0; i < name.length; i++) {
    h = (h << 5) - h + name.charCodeAt(i);
    h |= 0;
  }
  return h;
};

export const startGraphileWorker = async (opt: GraphileWorkerOpts): Promise<GraphileWorker> => {
  for (const c of opt.crons) validateCron(c.schedule);

  const parsedCronItems = parseCronItems(
    opt.crons.map((c) => ({
      task: `${CRON_TASK_PREFIX}${c.name}`,
      match: buildMatchPattern(c),
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

const buildMatchPattern = (c: CronSpec): string => {
  // graphile-worker supports cron strings only; ignore IANA tz for now (it's
  // honored by the runtime when graphile-worker grows native tz support).
  void c.timezone;
  return c.schedule;
};
