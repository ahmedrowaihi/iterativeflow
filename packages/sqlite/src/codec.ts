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

const column = (row: SqlRow, name: string): SqlValue => {
  const v = row[name];
  if (v === undefined) throw new Error(`sqlite: result has no column "${name}"`);
  return v;
};

/** @internal */
export const text = (row: SqlRow, name: string): string => {
  const v = column(row, name);
  if (!isString(v)) throw new Error(`sqlite: column "${name}" is not text`);
  return v;
};

/** @internal */
export const int = (row: SqlRow, name: string): number => {
  const v = column(row, name);
  const n = Number(v);
  if (v === null || !Number.isFinite(n))
    throw new Error(`sqlite: column "${name}" is not a number`);
  return n;
};

/** @internal */
export const optText = (row: SqlRow, name: string): string | undefined =>
  column(row, name) === null ? undefined : text(row, name);

/** @internal */
export const optInt = (row: SqlRow, name: string): number | undefined =>
  column(row, name) === null ? undefined : int(row, name);

const oneOf = <T extends string>(allowed: readonly T[], row: SqlRow, name: string): T =>
  decodeOneOf(allowed, text(row, name), `sqlite: column "${name}"`);

const json = (row: SqlRow, name: string): Json | undefined => {
  const raw = optText(row, name);
  return raw === undefined ? undefined : JSON.parse(raw);
};

const optDate = (row: SqlRow, name: string): Date | undefined => {
  const ms = optInt(row, name);
  return ms === undefined ? undefined : new Date(ms);
};

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
  error: decodeFlowError(json(r, "error"), `sqlite: column "error"`),
  idempotencyKey: optText(r, "idempotency_key"),
  tags: decodeTags(json(r, "tags"), `sqlite: column "tags"`),
  parentRunId: optText(r, "parent_run_id"),
  parentCursorKey: optText(r, "parent_cursor_key"),
  depth: int(r, "depth"),
  createdAt: new Date(int(r, "created_at")),
});

// The `shape` column predates the `call` rename.
/** @internal */
export const STEP_COLUMNS = "status, result, error, attempts, shape AS memo_call";

/** @internal */
export const mapStep = (r: SqlRow): StepOutcome => ({
  status: oneOf(STEP_STATUSES, r, "status"),
  result: json(r, "result"),
  error: decodeFlowError(json(r, "error"), `sqlite: column "error"`),
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
  nextRunAt: new Date(int(r, "next_run_at")),
  lastRunAt: optDate(r, "last_run_at"),
});
