import { sql } from "drizzle-orm";
import type { Pool } from "pg";
import type { CronSpec, Logger } from "../../engine/types";
import type { WorkflowDb } from "../../storage/db";
import { rowsOf } from "../../storage/drizzle/types";
import { asError } from "../../util/errors";

export const CRON_TASK_PREFIX = "cron:";

type RunCron = (name: string, fn: () => Promise<void>) => Promise<void>;

const wrapBody =
  (spec: CronSpec): (() => Promise<void>) =>
  async () => {
    if (spec.jitterMs && spec.jitterMs > 0) {
      await new Promise((r) => setTimeout(r, Math.floor(spec.jitterMs! * Math.random())));
    }
    await spec.run();
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

/**
 * Cron tick handler enforcing overlap policy. With `overlap: "allow"` it runs
 * immediately; otherwise it holds a session advisory lock so a tick is skipped
 * when a previous tick on any instance is still running. Jitter is applied
 * inside the lock.
 *
 * @internal
 */
export const buildCronHandler =
  (spec: CronSpec, logger: Logger, runCron: RunCron, pool: Pool) => async () => {
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
      logger.error(asError(err), { event: "flow.cron.failed", cron: spec.name });
      throw err;
    }
  };

/**
 * Best-effort purge of `cron:*` jobs whose task no longer matches a registered
 * cron — left behind when a cron is removed from code. graphile-worker stops
 * scheduling the removed cron but already-enqueued jobs linger with no handler.
 * Never throws: a reap failure must not block worker startup.
 *
 * @internal
 */
export const reapOrphanedCronJobs = async (
  db: WorkflowDb,
  schema: string,
  crons: CronSpec[],
  logger: Logger,
): Promise<void> => {
  const sch = sql.identifier(schema);
  const known = crons.map((c) => `${CRON_TASK_PREFIX}${c.name}`);
  const keepClause = known.length
    ? sql`AND task_identifier NOT IN (${sql.join(
        known.map((k) => sql`${k}`),
        sql`, `,
      )})`
    : sql``;
  try {
    const reaped = rowsOf<{ n: number }>(
      await db.execute(sql`
        SELECT count(*)::int AS n FROM ${sch}.complete_jobs(
          ARRAY(
            SELECT id FROM ${sch}.jobs
            WHERE task_identifier LIKE ${`${CRON_TASK_PREFIX}%`}
              AND locked_at IS NULL
              ${keepClause}
          )
        )
      `),
    );
    const count = reaped[0]?.n ?? 0;
    if (count > 0) logger.info("flow.cron.reaped", { count });
  } catch (err) {
    logger.error(asError(err), { event: "flow.cron.reap_failed" });
  }
};
