import { assertSqlIdentifier } from "@iterativeflow/core/backend";
import type { Sql } from "#sql";

/** @internal */
export const tables = (schema: string) => {
  assertSqlIdentifier(schema);
  return {
    run: `"${schema}".run`,
    step: `"${schema}".step`,
    job: `"${schema}".job`,
    timer: `"${schema}".timer`,
    signal: `"${schema}".signal`,
    event: `"${schema}".event`,
    cron: `"${schema}".cron`,
  };
};

export type Tables = ReturnType<typeof tables>;

/**
 * DDL for one schema. `run` carries the durable state; `step` is the exactly-once memo (PK
 * `(run_id, cursor_key)` is the first-writer-wins guard); `job` is the lease-CAS queue;
 * `timer` is the durable-deadline set. Ids are opaque `text` supplied by the runtime's
 * `IdGen` — the schema never assumes a UUID shape or a DB-side default, so callers are free
 * to use ULIDs/KSUIDs/etc. The `run` FK on `step` has NO cascade: runs are never deleted, so
 * the reference exists only to reject a step whose run doesn't exist.
 */
export const ddl = (schema: string): string => {
  const t = tables(schema);
  return `
CREATE SCHEMA IF NOT EXISTS "${schema}";

CREATE TABLE IF NOT EXISTS ${t.run} (
  id                text PRIMARY KEY,
  seq               bigint GENERATED ALWAYS AS IDENTITY,
  name              text NOT NULL,
  version           int  NOT NULL,
  status            text NOT NULL,
  input             jsonb,
  output            jsonb,
  error             jsonb,
  attempts          int  NOT NULL DEFAULT 0,
  idempotency_key   text,
  tags              text[],
  parent_run_id     text,
  parent_cursor_key text,
  depth             int  NOT NULL DEFAULT 0,
  join_remaining    int  NOT NULL DEFAULT 0,
  created_at        timestamptz NOT NULL DEFAULT now()
);
-- Idempotent upgrades for tables created before these columns existed (applySchema runs on boot).
ALTER TABLE ${t.run} ADD COLUMN IF NOT EXISTS join_remaining int NOT NULL DEFAULT 0;
ALTER TABLE ${t.run} ADD COLUMN IF NOT EXISTS depth int NOT NULL DEFAULT 0;
ALTER TABLE ${t.run} ADD COLUMN IF NOT EXISTS created_at timestamptz NOT NULL DEFAULT now();
-- Retention sweep scans terminal runs by age.
CREATE INDEX IF NOT EXISTS run_created ON ${t.run} (created_at);
CREATE INDEX IF NOT EXISTS run_parent ON ${t.run} (parent_run_id) WHERE parent_run_id IS NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS run_idem
  ON ${t.run} (name, version, idempotency_key) WHERE idempotency_key IS NOT NULL;
CREATE INDEX IF NOT EXISTS run_status ON ${t.run} (status);

CREATE TABLE IF NOT EXISTS ${t.step} (
  run_id      text NOT NULL REFERENCES ${t.run}(id),
  cursor_key  text NOT NULL,
  status      text NOT NULL,
  result      jsonb,
  error       jsonb,
  attempts    int  NOT NULL,
  shape       text,
  PRIMARY KEY (run_id, cursor_key)
);

-- vacuum this high-churn table at 2% dead rows (vs the 20% default); on create so a later ALTER isn't reset.
CREATE TABLE IF NOT EXISTS ${t.job} (
  run_id        text PRIMARY KEY,
  run_at        timestamptz NOT NULL DEFAULT now(),
  priority      int NOT NULL DEFAULT 0,
  version       bigint NOT NULL DEFAULT 0,
  lease_token   text,
  lease_expires timestamptz
) WITH (autovacuum_vacuum_scale_factor = 0.02);
CREATE INDEX IF NOT EXISTS job_claimable ON ${t.job} (priority, run_at) WHERE lease_expires IS NULL;

CREATE TABLE IF NOT EXISTS ${t.timer} (
  run_id  text PRIMARY KEY,
  fire_at timestamptz NOT NULL
);
CREATE INDEX IF NOT EXISTS timer_due ON ${t.timer} (fire_at);

CREATE TABLE IF NOT EXISTS ${t.signal} (
  id       text PRIMARY KEY,
  run_id   text NOT NULL REFERENCES ${t.run}(id),
  name     text NOT NULL,
  payload  jsonb,
  seq      bigint GENERATED ALWAYS AS IDENTITY,
  idem_key text
);
CREATE INDEX IF NOT EXISTS signal_inbox ON ${t.signal} (run_id, seq);
CREATE UNIQUE INDEX IF NOT EXISTS signal_idem
  ON ${t.signal} (run_id, idem_key) WHERE idem_key IS NOT NULL;

CREATE TABLE IF NOT EXISTS ${t.event} (
  seq    bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  run_id text NOT NULL,
  type   text NOT NULL,
  at     timestamptz NOT NULL,
  data   jsonb
);
CREATE INDEX IF NOT EXISTS event_run ON ${t.event} (run_id, seq);

CREATE TABLE IF NOT EXISTS ${t.cron} (
  name         text PRIMARY KEY,
  schedule     text NOT NULL,
  flow_name    text NOT NULL,
  flow_version int  NOT NULL,
  input        jsonb,
  overlap      text NOT NULL DEFAULT 'allow',
  next_run_at  timestamptz NOT NULL,
  last_run_at  timestamptz
);
CREATE INDEX IF NOT EXISTS cron_due ON ${t.cron} (next_run_at);

CREATE OR REPLACE FUNCTION "${schema}".pending_work(flow_names text[] DEFAULT NULL, as_of timestamptz DEFAULT now())
RETURNS bigint LANGUAGE sql STABLE AS $$
  SELECT
    (SELECT count(*) FROM ${t.job} j LEFT JOIN ${t.run} r ON r.id = j.run_id
       WHERE j.run_at <= as_of AND (j.lease_expires IS NULL OR j.lease_expires <= as_of)
         AND (flow_names IS NULL OR r.name = ANY(flow_names)))
  + (SELECT count(*) FROM ${t.timer} tm LEFT JOIN ${t.run} r ON r.id = tm.run_id
       WHERE tm.fire_at <= as_of AND (flow_names IS NULL OR r.name = ANY(flow_names)))
  + (SELECT count(*) FROM ${t.cron} c
       WHERE c.next_run_at <= as_of AND (flow_names IS NULL OR c.flow_name = ANY(flow_names)))
$$;
`;
};

/** Apply the schema DDL. Idempotent — safe to run on every boot. */
export const applySchema = async (sql: Sql, schema = "workflow"): Promise<void> => {
  await sql.query(ddl(schema));
};
