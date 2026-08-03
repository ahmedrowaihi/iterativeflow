import type { CronRow, DeliveredSignal, RunRow, StepOutcome } from "@iterativeflow/core/backend";

/** @internal */
export const j = (value: unknown): string | null =>
  value === undefined ? null : JSON.stringify(value);

const p = <T>(text: string | null): T | undefined =>
  text === null ? undefined : (JSON.parse(text) as T);

const orUndef = <T>(v: T | null): T | undefined => (v === null ? undefined : v);

export interface RunRecord {
  id: string;
  name: string;
  version: number;
  status: RunRow["status"];
  input: string | null;
  output: string | null;
  error: string | null;
  attempts: number;
  idempotency_key: string | null;
  tags: string | null;
  parent_run_id: string | null;
  parent_cursor_key: string | null;
  depth: number;
  created_at: number;
}

export const mapRun = (r: RunRecord): RunRow => ({
  id: r.id,
  name: r.name,
  version: r.version,
  status: r.status,
  input: p(r.input),
  attempts: r.attempts,
  output: p(r.output),
  error: p<RunRow["error"]>(r.error),
  idempotencyKey: orUndef(r.idempotency_key),
  tags: p<string[]>(r.tags),
  parentRunId: orUndef(r.parent_run_id),
  parentCursorKey: orUndef(r.parent_cursor_key),
  depth: r.depth,
  createdAt: new Date(r.created_at),
});

export interface StepRecord {
  status: StepOutcome["status"];
  result: string | null;
  error: string | null;
  attempts: number;
  shape?: string | null;
}

export const mapStep = (r: StepRecord): StepOutcome => ({
  status: r.status,
  result: p(r.result),
  error: p<StepOutcome["error"]>(r.error),
  attempts: r.attempts,
  shape: orUndef(r.shape ?? null),
});

export interface SignalRecord {
  id: string;
  name: string;
  payload: string | null;
}

export const mapSignal = (r: SignalRecord): DeliveredSignal => ({
  id: r.id,
  name: r.name,
  payload: p(r.payload),
});

export interface CronRecord {
  name: string;
  schedule: string;
  flow_name: string;
  flow_version: number;
  input: string | null;
  overlap: "allow" | "skip";
  next_run_at: number;
  last_run_at: number | null;
}

export const mapCron = (r: CronRecord): CronRow => ({
  name: r.name,
  schedule: r.schedule,
  flowName: r.flow_name,
  flowVersion: r.flow_version,
  input: p(r.input),
  overlap: r.overlap,
  nextRunAt: new Date(r.next_run_at),
  lastRunAt: r.last_run_at === null ? undefined : new Date(r.last_run_at),
});
