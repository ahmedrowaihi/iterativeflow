import type { FlowError, RunRow, StepOutcome } from "@iterativeflow/core/backend";

// User values are JSON-encoded (not left to the document client) for an exact round-trip and
// to sidestep Dynamo's rejection of empty maps / unsupported nested scalars.
/** @internal */
export const enc = (value: unknown): string | undefined =>
  value === undefined ? undefined : JSON.stringify(value);

/** @internal */
export const dec = (raw: unknown): unknown =>
  raw === undefined || raw === null ? undefined : JSON.parse(raw as string);

const asFlowError = (v: unknown): FlowError | undefined =>
  v && typeof v === "object" && "code" in v && "message" in v ? (v as FlowError) : undefined;

let counter = 0;
// Process-monotonic order token (Postgres uses a bigint IDENTITY); a multi-process deployment
// would source it from a Dynamo atomic counter.
/** @internal */
export const nextSeq = (): number => ++counter;

export interface RunItem {
  id: string;
  name: string;
  version: number;
  status: RunRow["status"];
  input?: string;
  output?: string;
  error?: string;
  attempts: number;
  idempotencyKey?: string;
  tags?: string[];
  parentRunId?: string;
  parentCursorKey?: string;
  depth?: number;
  createdAt?: string;
  seq: number;
}

export const mapRun = (r: RunItem): RunRow => ({
  id: r.id,
  name: r.name,
  version: r.version,
  status: r.status,
  input: dec(r.input),
  attempts: r.attempts,
  output: dec(r.output),
  error: asFlowError(dec(r.error)),
  idempotencyKey: r.idempotencyKey,
  tags: r.tags,
  parentRunId: r.parentRunId,
  parentCursorKey: r.parentCursorKey,
  depth: r.depth ?? 0,
  createdAt: r.createdAt ? new Date(r.createdAt) : undefined,
});

export interface CronItem {
  cronName: string;
  schedule: string;
  flowName: string;
  flowVersion: number;
  cronInput?: string;
  overlap: "allow" | "skip";
  nextRunAt: number;
  lastRunAt?: number;
}

export interface StepItem {
  status: StepOutcome["status"];
  result?: string;
  error?: string;
  attempts: number;
  shape?: string;
}

export const mapStep = (r: StepItem): StepOutcome => ({
  status: r.status,
  result: dec(r.result),
  error: asFlowError(dec(r.error)),
  attempts: r.attempts,
  shape: r.shape,
});

export type RunPartitionItem =
  | ({ type: "run" } & RunItem)
  | ({ type: "step"; cursorKey: string } & StepItem)
  | { type: "signal"; pk: string; sk: string; name: string; payload?: string }
  | { type: "sigidem" };
