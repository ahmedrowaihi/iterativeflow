import { assertSqlIdentifier } from "@iterativeflow/core/backend";
import type { Sql } from "#sql";

/** @internal */
export const tables = (prefix: string) => {
  assertSqlIdentifier(prefix);
  return {
    run: `\`${prefix}run\``,
    step: `\`${prefix}step\``,
    job: `\`${prefix}job\``,
    timer: `\`${prefix}timer\``,
    signal: `\`${prefix}signal\``,
    cron: `\`${prefix}cron\``,
  };
};

export type Tables = ReturnType<typeof tables>;

/**
 * DDL for the MySQL backend (InnoDB). `run` carries durable state; `step` is the exactly-once memo
 * (PK `(run_id, cursor_key)`); `job` is the lease queue; `timer` the durable-deadline set. Timestamps
 * are BIGINT epoch ms; JSON columns are LONGTEXT; `seq` is AUTO_INCREMENT (insertion order, since
 * MySQL has no implicit rowid). Indexed string columns are `VARCHAR(191)` to stay under the utf8mb4
 * index-key limit. The idempotency/signal-dedup UNIQUE KEYs need no partial `WHERE`: MySQL treats
 * NULLs as distinct, so unkeyed rows never collide.
 *
 * Also creates the `pending_work` function, so applying this needs `CREATE ROUTINE` as well as
 * `CREATE TABLE`. MySQL has no `CREATE OR REPLACE FUNCTION`, so that one statement is a `DROP` +
 * `CREATE` rather than `IF NOT EXISTS` — a scaler polling the function across a boot can see one
 * failed read.
 */
export const ddl = (prefix = ""): string[] => {
  const t = tables(prefix);
  return [
    `CREATE TABLE IF NOT EXISTS ${t.run} (
      id                VARCHAR(191) PRIMARY KEY,
      seq               BIGINT AUTO_INCREMENT UNIQUE,
      name              VARCHAR(191) NOT NULL,
      version           INT NOT NULL,
      status            VARCHAR(32) NOT NULL,
      input             LONGTEXT,
      output            LONGTEXT,
      error             LONGTEXT,
      attempts          INT NOT NULL DEFAULT 0,
      idempotency_key   VARCHAR(191),
      tags              LONGTEXT,
      parent_run_id     VARCHAR(191),
      parent_cursor_key VARCHAR(191),
      depth             INT NOT NULL DEFAULT 0,
      join_remaining    INT NOT NULL DEFAULT 0,
      created_at        BIGINT NOT NULL,
      UNIQUE KEY run_idem (name, version, idempotency_key),
      KEY run_created (created_at),
      KEY run_parent (parent_run_id),
      KEY run_status (status)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`,

    `CREATE TABLE IF NOT EXISTS ${t.step} (
      run_id     VARCHAR(191) NOT NULL,
      cursor_key VARCHAR(191) NOT NULL,
      status     VARCHAR(32) NOT NULL,
      result     LONGTEXT,
      error      LONGTEXT,
      attempts   INT NOT NULL,
      shape      VARCHAR(191),
      PRIMARY KEY (run_id, cursor_key)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`,

    `CREATE TABLE IF NOT EXISTS ${t.job} (
      run_id        VARCHAR(191) PRIMARY KEY,
      run_at        BIGINT NOT NULL,
      priority      INT NOT NULL DEFAULT 0,
      version       BIGINT NOT NULL DEFAULT 0,
      lease_token   VARCHAR(191),
      lease_expires BIGINT,
      KEY job_claimable (priority, run_at)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`,

    `CREATE TABLE IF NOT EXISTS ${t.timer} (
      run_id  VARCHAR(191) PRIMARY KEY,
      fire_at BIGINT NOT NULL,
      KEY timer_due (fire_at)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`,

    `CREATE TABLE IF NOT EXISTS ${t.signal} (
      id       VARCHAR(191) PRIMARY KEY,
      seq      BIGINT AUTO_INCREMENT UNIQUE,
      run_id   VARCHAR(191) NOT NULL,
      name     VARCHAR(191) NOT NULL,
      payload  LONGTEXT,
      idem_key VARCHAR(191),
      consumed TINYINT(1) NOT NULL DEFAULT 0,
      KEY signal_inbox (run_id, seq),
      UNIQUE KEY signal_idem (run_id, idem_key)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`,

    `CREATE TABLE IF NOT EXISTS ${t.cron} (
      name         VARCHAR(191) PRIMARY KEY,
      schedule     VARCHAR(255) NOT NULL,
      flow_name    VARCHAR(191) NOT NULL,
      flow_version INT NOT NULL,
      input        LONGTEXT,
      overlap      VARCHAR(16) NOT NULL DEFAULT 'allow',
      next_run_at  BIGINT NOT NULL,
      last_run_at  BIGINT,
      KEY cron_due (next_run_at)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`,

    `DROP FUNCTION IF EXISTS \`${prefix}pending_work\``,
    `CREATE FUNCTION \`${prefix}pending_work\`(flow_names JSON, as_of BIGINT)
     RETURNS BIGINT READS SQL DATA
     RETURN (
       (SELECT COUNT(*) FROM ${t.job} j
          WHERE j.run_at <= as_of AND (j.lease_expires IS NULL OR j.lease_expires <= as_of)
            AND (flow_names IS NULL OR (JSON_LENGTH(flow_names) > 0 AND NOT EXISTS (
                  SELECT 1 FROM ${t.run} r
                   WHERE r.id = j.run_id AND NOT JSON_CONTAINS(flow_names, JSON_QUOTE(r.name))))))
     + (SELECT COUNT(*) FROM ${t.timer} tm
          WHERE tm.fire_at <= as_of
            AND (flow_names IS NULL OR (JSON_LENGTH(flow_names) > 0 AND NOT EXISTS (
                  SELECT 1 FROM ${t.run} r
                   WHERE r.id = tm.run_id AND NOT JSON_CONTAINS(flow_names, JSON_QUOTE(r.name))))))
     + (SELECT COUNT(*) FROM ${t.cron} c
          WHERE c.next_run_at <= as_of AND (flow_names IS NULL OR JSON_CONTAINS(flow_names, JSON_QUOTE(c.flow_name))))
     )`,
  ];
};

// MySQL has no `ADD COLUMN IF NOT EXISTS`, and applySchema runs on every boot, so check first.
const addSignalConsumed = async (sql: Sql, t: Tables): Promise<void> => {
  const cols = await sql.query<{ n: number }>(
    `SELECT COUNT(*) AS n FROM information_schema.columns
      WHERE table_schema = DATABASE() AND table_name = ? AND column_name = 'consumed'`,
    [t.signal.replace(/`/g, "")],
  );
  if (Number(cols[0]?.n ?? 0) > 0) return;
  await sql.query(`ALTER TABLE ${t.signal} ADD COLUMN consumed TINYINT(1) NOT NULL DEFAULT 0`);
};

/** Apply the schema DDL (idempotent). Run once before use. */
export const applySchema = async (sql: Sql, prefix = ""): Promise<void> => {
  for (const stmt of ddl(prefix)) await sql.query(stmt);
  await addSignalConsumed(sql, tables(prefix));
};
