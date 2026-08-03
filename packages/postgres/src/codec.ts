import type { RunRow, StepOutcome } from "@iterativeflow/core/backend";

/** @internal */
export const j = (value: unknown): string | null =>
  value === undefined ? null : JSON.stringify(value);

const orUndef = <T>(v: T | null): T | undefined => (v === null ? undefined : v);

export interface RunRecord {
  id: string;
  name: string;
  version: number;
  status: RunRow["status"];
  input: unknown;
  output: unknown;
  error: RunRow["error"] | null;
  attempts: number;
  idempotency_key: string | null;
  tags: string[] | null;
  parent_run_id: string | null;
  parent_cursor_key: string | null;
  depth: number;
  created_at: Date;
}

export const mapRun = (r: RunRecord): RunRow => ({
  id: r.id,
  name: r.name,
  version: r.version,
  status: r.status,
  input: r.input,
  attempts: r.attempts,
  output: orUndef(r.output),
  error: orUndef(r.error),
  idempotencyKey: orUndef(r.idempotency_key),
  tags: orUndef(r.tags),
  parentRunId: orUndef(r.parent_run_id),
  parentCursorKey: orUndef(r.parent_cursor_key),
  depth: r.depth,
  createdAt: r.created_at,
});

export interface StepRecord {
  status: StepOutcome["status"];
  result: unknown;
  error: StepOutcome["error"] | null;
  attempts: number;
  shape?: string | null;
}

export const mapStep = (r: StepRecord): StepOutcome => ({
  status: r.status,
  result: orUndef(r.result),
  error: orUndef(r.error),
  attempts: r.attempts,
  shape: orUndef(r.shape),
});
