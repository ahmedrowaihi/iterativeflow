import type {
  CronRow,
  DeliveredSignal,
  FlowError,
  RunRow,
  RunSpec,
  StepOutcome,
} from "@iterativeflow/core/backend";
import { ObjectId } from "mongodb";

export interface RunDoc {
  _id: string;
  name: string;
  version: number;
  status: RunRow["status"];
  input: unknown;
  output?: unknown;
  error?: FlowError;
  attempts: number;
  idempotency_key?: string;
  tags?: string[];
  parent_run_id?: string;
  parent_cursor_key?: string;
  depth?: number;
  join_remaining?: number;
  created_at: number;
  ord: ObjectId;
}

export interface StepDoc {
  _id: string;
  run_id: string;
  cursor_key: string;
  status: StepOutcome["status"];
  result?: unknown;
  error?: FlowError;
  attempts: number;
  shape?: string;
}

export interface SignalDoc {
  _id: string;
  run_id: string;
  name: string;
  payload: unknown;
  idem_key?: string;
  ord: ObjectId;
}

export interface CronDoc {
  _id: string;
  schedule: string;
  flow_name: string;
  flow_version: number;
  input: unknown;
  overlap: "allow" | "skip";
  next_run_at: number;
  last_run_at: number | null;
}

/** @internal */
export const mapRun = (d: RunDoc): RunRow => ({
  id: d._id,
  name: d.name,
  version: d.version,
  status: d.status,
  input: d.input,
  attempts: d.attempts,
  output: d.output,
  error: d.error,
  idempotencyKey: d.idempotency_key,
  tags: d.tags,
  parentRunId: d.parent_run_id,
  parentCursorKey: d.parent_cursor_key,
  depth: d.depth ?? 0,
  createdAt: new Date(d.created_at),
});

/** @internal */
export const mapStep = (d: StepDoc): StepOutcome => ({
  status: d.status,
  result: d.result,
  error: d.error,
  attempts: d.attempts,
  shape: d.shape,
});

/** @internal */
export const mapSignal = (d: SignalDoc): DeliveredSignal => ({
  id: d._id,
  name: d.name,
  payload: d.payload,
});

/** @internal */
export const mapCron = (d: CronDoc): CronRow => ({
  name: d._id,
  schedule: d.schedule,
  flowName: d.flow_name,
  flowVersion: d.flow_version,
  input: d.input,
  overlap: d.overlap,
  nextRunAt: new Date(d.next_run_at),
  lastRunAt: d.last_run_at === null ? undefined : new Date(d.last_run_at),
});

/** @internal */
export const buildRunDoc = (spec: RunSpec, runId: string, ord: ObjectId): RunDoc => ({
  _id: runId,
  name: spec.name,
  version: spec.version,
  status: "pending",
  input: spec.input,
  attempts: 0,
  depth: spec.depth ?? 0,
  created_at: spec.createdAt ? spec.createdAt.getTime() : Date.now(),
  ord,
  // Absent optionals stay absent: the sparse unique indexes must not index a null idempotency_key.
  ...(spec.idempotencyKey !== undefined && { idempotency_key: spec.idempotencyKey }),
  ...(spec.tags && { tags: [...spec.tags] }),
  ...(spec.parentRunId !== undefined && { parent_run_id: spec.parentRunId }),
  ...(spec.parentCursorKey !== undefined && { parent_cursor_key: spec.parentCursorKey }),
});
