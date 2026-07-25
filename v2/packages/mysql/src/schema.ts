import { assertSqlIdentifier } from "@iterativeflow/core/backend";
import type { Sql } from "#sql";

/** @internal */
export const tables = (prefix: string) => ({
  run: `\`${prefix}run\``,
  step: `\`${prefix}step\``,
  job: `\`${prefix}job\``,
  timer: `\`${prefix}timer\``,
  signal: `\`${prefix}signal\``,
  cron: `\`${prefix}cron\``,
});

export type Tables = ReturnType<typeof tables>;

/**
 * DDL for the MySQL backend (InnoDB). `run` carries durable state; `step` is the exactly-once memo
 * (PK `(run_id, cursor_key)`); `job` is the lease queue; `timer` the durable-deadline set. Timestamps
 * are BIGINT epoch ms; JSON columns are LONGTEXT; `seq` is AUTO_INCREMENT (insertion order, since
 * MySQL has no implicit rowid). Indexed string columns are `VARCHAR(191)` to stay under the utf8mb4
 * index-key limit. The idempotency/signal-dedup UNIQUE KEYs need no partial `WHERE`: MySQL treats
 * NULLs as distinct, so unkeyed rows never collide.
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
  ];
};

/** Apply the schema DDL (idempotent). Run once before use. */
export const applySchema = async (sql: Sql, prefix = ""): Promise<void> => {
  assertSqlIdentifier(prefix);
  for (const stmt of ddl(prefix)) await sql.query(stmt);
};
