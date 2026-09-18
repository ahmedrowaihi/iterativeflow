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

const isString = (v: SqlValue | undefined): v is string => v === String(v);

const cell = (row: SqlRow, column: string): SqlValue => {
  const v = row[column];
  if (v === undefined) throw new Error(`postgres: result has no column "${column}"`);
  return v;
};

/** @internal */
export const text = (row: SqlRow, column: string): string => {
  const v = cell(row, column);
  if (!isString(v)) throw new Error(`postgres: column "${column}" is not text`);
  return v;
};

// node-postgres hands BIGINT back as a string; every numeric read goes through Number.
/** @internal */
export const int = (row: SqlRow, column: string): number => {
  const v = cell(row, column);
  const n = Number(v);
  if (v === null || v instanceof Date || !Number.isFinite(n)) {
    throw new Error(`postgres: column "${column}" is not a number`);
  }
  return n;
};

// A `bigint` arrives as text by default, or as a number under a custom int8 type parser.
/** @internal */
export const bigintText = (row: SqlRow, column: string): string => {
  const v = cell(row, column);
  if (isString(v)) return v;
  if (Number.isInteger(v)) return String(v);
  throw new Error(`postgres: column "${column}" is not a bigint`);
};

const optText = (row: SqlRow, column: string): string | undefined =>
  cell(row, column) === null ? undefined : text(row, column);

/** @internal */
export const optDate = (row: SqlRow, column: string): Date | undefined => {
  const v = cell(row, column);
  if (v === null) return undefined;
  // A custom timestamptz type parser may hand back the raw text instead of a Date.
  const at = v instanceof Date ? v : isString(v) ? new Date(v) : undefined;
  if (!at || Number.isNaN(at.getTime())) {
    throw new Error(`postgres: column "${column}" is not a timestamp`);
  }
  return at;
};

/** @internal */
export const date = (row: SqlRow, column: string): Date => {
  const v = optDate(row, column);
  if (v === undefined) throw new Error(`postgres: column "${column}" is NULL`);
  return v;
};

/** @internal */
export const json = (row: SqlRow, column: string): Json | undefined => {
  const v = cell(row, column);
  if (v instanceof Date) throw new Error(`postgres: column "${column}" is not JSON`);
  return v === null ? undefined : v;
};

const oneOf = <T extends string>(allowed: readonly T[], row: SqlRow, column: string): T =>
  decodeOneOf(allowed, text(row, column), `postgres: column "${column}"`);

/** @internal */
export const runStatus = (r: SqlRow): RunStatus => oneOf(RUN_STATUSES, r, "status");

/** @internal */
export const mapRun = (r: SqlRow): RunRow => ({
  id: text(r, "id"),
  name: text(r, "name"),
  version: int(r, "version"),
  status: runStatus(r),
  input: json(r, "input"),
  attempts: int(r, "attempts"),
  output: json(r, "output"),
  error: decodeFlowError(json(r, "error"), `postgres: column "error"`),
  idempotencyKey: optText(r, "idempotency_key"),
  tags: decodeTags(json(r, "tags"), `postgres: column "tags"`),
  parentRunId: optText(r, "parent_run_id"),
  parentCursorKey: optText(r, "parent_cursor_key"),
  depth: int(r, "depth"),
  createdAt: date(r, "created_at"),
});

// The `shape` column predates the `call` rename.
/** @internal */
export const STEP_COLUMNS = "status, result, error, attempts, shape AS memo_call";

/** @internal */
export const mapStep = (r: SqlRow): StepOutcome => ({
  status: oneOf(STEP_STATUSES, r, "status"),
  result: json(r, "result"),
  error: decodeFlowError(json(r, "error"), `postgres: column "error"`),
  attempts: int(r, "attempts"),
  call: optText(r, "memo_call"),
});

/** @internal */
export const mapSignal = (r: SqlRow): DeliveredSignal => ({
  id: text(r, "id"),
  name: text(r, "name"),
  payload: json(r, "payload"),
});

/** @internal */
export const mapCron = (r: SqlRow): CronRow => ({
  name: text(r, "name"),
  schedule: text(r, "schedule"),
  flowName: text(r, "flow_name"),
  flowVersion: int(r, "flow_version"),
  input: json(r, "input"),
  overlap: oneOf(CRON_OVERLAPS, r, "overlap"),
  nextRunAt: date(r, "next_run_at"),
  lastRunAt: optDate(r, "last_run_at"),
});
