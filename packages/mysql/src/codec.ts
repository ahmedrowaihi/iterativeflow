import {
  type CronRow,
  type DeliveredSignal,
  type Json,
  type RunRow,
  type RunStatus,
  type StepOutcome,
  CRON_OVERLAPS,
  RUN_STATUSES,
  STEP_STATUSES,
  decodeFlowError,
  decodeOneOf,
  decodeTags,
} from "@iterativeflow/core/backend";
import type { SqlRow, SqlValue } from "#sql";

/** @internal */
export const j = <T>(value: T): string | null =>
  value === undefined ? null : JSON.stringify(value);

const cell = (row: SqlRow, column: string): SqlValue => {
  const value = row[column];
  if (value === undefined) throw new Error(`mysql: result has no column "${column}"`);
  return value;
};

/** @internal */
export const textOrNull = (row: SqlRow, column: string): string | null => {
  const value = cell(row, column);
  return value === null ? null : String(value);
};

/** @internal */
export const text = (row: SqlRow, column: string): string => {
  const value = textOrNull(row, column);
  if (value === null) throw new Error(`mysql: column "${column}" is NULL`);
  return value;
};

// mysql2 hands BIGINT columns back as strings; coerce every numeric read through Number.
/** @internal */
export const intOrNull = (row: SqlRow, column: string): number | null => {
  const value = cell(row, column);
  return value === null ? null : Number(value);
};

/** @internal */
export const int = (row: SqlRow, column: string): number => Number(text(row, column));

const oneOf = <T extends string>(allowed: readonly T[], row: SqlRow, column: string): T =>
  decodeOneOf(allowed, text(row, column), `mysql: column "${column}"`);

/** @internal */
export const runStatus = (row: SqlRow): RunStatus => oneOf(RUN_STATUSES, row, "status");

const optionalText = (row: SqlRow, column: string): string | undefined =>
  textOrNull(row, column) ?? undefined;

const json = (row: SqlRow, column: string): Json | undefined => {
  const value = textOrNull(row, column);
  return value === null ? undefined : JSON.parse(value);
};

const at = (row: SqlRow, column: string): Date => new Date(int(row, column));

export const mapRun = (r: SqlRow): RunRow => ({
  id: text(r, "id"),
  name: text(r, "name"),
  version: int(r, "version"),
  status: runStatus(r),
  input: json(r, "input"),
  attempts: int(r, "attempts"),
  output: json(r, "output"),
  error: decodeFlowError(json(r, "error"), `mysql: column "error"`),
  idempotencyKey: optionalText(r, "idempotency_key"),
  tags: decodeTags(json(r, "tags"), `mysql: column "tags"`),
  parentRunId: optionalText(r, "parent_run_id"),
  parentCursorKey: optionalText(r, "parent_cursor_key"),
  depth: int(r, "depth"),
  createdAt: at(r, "created_at"),
});

// The `shape` column predates the `call` rename; `call` itself is reserved in MySQL.
/** @internal */
export const STEP_COLUMNS = "status, result, error, attempts, shape AS memo_call";

export const mapStep = (r: SqlRow): StepOutcome => ({
  status: oneOf(STEP_STATUSES, r, "status"),
  result: json(r, "result"),
  error: decodeFlowError(json(r, "error"), `mysql: column "error"`),
  attempts: int(r, "attempts"),
  call: optionalText(r, "memo_call"),
});

export const mapSignal = (r: SqlRow): DeliveredSignal => ({
  id: text(r, "id"),
  name: text(r, "name"),
  payload: json(r, "payload"),
});

export const mapCron = (r: SqlRow): CronRow => {
  const lastRunAt = intOrNull(r, "last_run_at");
  return {
    name: text(r, "name"),
    schedule: text(r, "schedule"),
    flowName: text(r, "flow_name"),
    flowVersion: int(r, "flow_version"),
    input: json(r, "input"),
    overlap: oneOf(CRON_OVERLAPS, r, "overlap"),
    nextRunAt: at(r, "next_run_at"),
    lastRunAt: lastRunAt === null ? undefined : new Date(lastRunAt),
  };
};
