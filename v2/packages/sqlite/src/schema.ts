import { assertSqlIdentifier } from "@iterativeflow/core/backend";
import type { Sql } from "#sql";

/** @internal */
export const tables = (prefix: string) => ({
  run: `${prefix}run`,
  step: `${prefix}step`,
  job: `${prefix}job`,
  timer: `${prefix}timer`,
  signal: `${prefix}signal`,
  cron: `${prefix}cron`,
});

export type Tables = ReturnType<typeof tables>;

/**
 * DDL for the SQLite backend. `run` carries durable state; `step` is the exactly-once memo (PK
 * `(run_id, cursor_key)` is the first-writer-wins guard); `job` is the lease queue; `timer` the
 * durable-deadline set. All timestamps are INTEGER epoch ms; JSON columns are TEXT. Insertion order
 * (listing, inbox, orphan) reads the implicit `rowid`, so no explicit seq column is needed. Ids are
 * opaque `text` from the runtime's `IdGen`.
 */
export const ddl = (prefix = ""): string => {
  const t = tables(prefix);
  return `
CREATE TABLE IF NOT EXISTS ${t.run} (
  id                TEXT PRIMARY KEY,
  name              TEXT NOT NULL,
  version           INTEGER NOT NULL,
  status            TEXT NOT NULL,
  input             TEXT,
  output            TEXT,
  error             TEXT,
  attempts          INTEGER NOT NULL DEFAULT 0,
  idempotency_key   TEXT,
  tags              TEXT,
  parent_run_id     TEXT,
  parent_cursor_key TEXT,
  depth             INTEGER NOT NULL DEFAULT 0,
  join_remaining    INTEGER NOT NULL DEFAULT 0,
  created_at        INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS ${t.run}_created ON ${t.run} (created_at);
CREATE INDEX IF NOT EXISTS ${t.run}_parent ON ${t.run} (parent_run_id) WHERE parent_run_id IS NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS ${t.run}_idem
  ON ${t.run} (name, version, idempotency_key) WHERE idempotency_key IS NOT NULL;
CREATE INDEX IF NOT EXISTS ${t.run}_status ON ${t.run} (status);

CREATE TABLE IF NOT EXISTS ${t.step} (
  run_id     TEXT NOT NULL REFERENCES ${t.run}(id),
  cursor_key TEXT NOT NULL,
  status     TEXT NOT NULL,
  result     TEXT,
  error      TEXT,
  attempts   INTEGER NOT NULL,
  shape      TEXT,
  PRIMARY KEY (run_id, cursor_key)
);

CREATE TABLE IF NOT EXISTS ${t.job} (
  run_id        TEXT PRIMARY KEY,
  run_at        INTEGER NOT NULL,
  priority      INTEGER NOT NULL DEFAULT 0,
  version       INTEGER NOT NULL DEFAULT 0,
  lease_token   TEXT,
  lease_expires INTEGER
);
CREATE INDEX IF NOT EXISTS ${t.job}_claimable ON ${t.job} (priority, run_at) WHERE lease_expires IS NULL;

CREATE TABLE IF NOT EXISTS ${t.timer} (
  run_id  TEXT PRIMARY KEY,
  fire_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS ${t.timer}_due ON ${t.timer} (fire_at);

CREATE TABLE IF NOT EXISTS ${t.signal} (
  id       TEXT PRIMARY KEY,
  run_id   TEXT NOT NULL REFERENCES ${t.run}(id),
  name     TEXT NOT NULL,
  payload  TEXT,
  idem_key TEXT
);
CREATE INDEX IF NOT EXISTS ${t.signal}_inbox ON ${t.signal} (run_id);
CREATE UNIQUE INDEX IF NOT EXISTS ${t.signal}_idem
  ON ${t.signal} (run_id, idem_key) WHERE idem_key IS NOT NULL;

CREATE TABLE IF NOT EXISTS ${t.cron} (
  name         TEXT PRIMARY KEY,
  schedule     TEXT NOT NULL,
  flow_name    TEXT NOT NULL,
  flow_version INTEGER NOT NULL,
  input        TEXT,
  overlap      TEXT NOT NULL DEFAULT 'allow',
  next_run_at  INTEGER NOT NULL,
  last_run_at  INTEGER
);
CREATE INDEX IF NOT EXISTS ${t.cron}_due ON ${t.cron} (next_run_at);
`;
};

/** Apply the schema DDL (idempotent). Splits on `;` because libsql executes one statement per call. */
export const applySchema = async (sql: Sql, prefix = ""): Promise<void> => {
  assertSqlIdentifier(prefix);
  for (const stmt of ddl(prefix).split(";")) {
    const s = stmt.trim();
    if (s) await sql.query(s);
  }
};
