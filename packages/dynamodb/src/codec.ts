import type { GetCommandOutput } from "@aws-sdk/lib-dynamodb";
import {
  type FlowError,
  type Json,
  type RunRow,
  type StepOutcome,
  CRON_OVERLAPS,
  RUN_STATUSES,
  STEP_STATUSES,
  decodeFlowError,
  decodeOneOf,
  decodeTags,
} from "@iterativeflow/core/backend";

/** @internal */
export type DocItem = NonNullable<GetCommandOutput["Item"]>;

// User values are JSON-encoded (not left to the document client) for an exact round-trip and
// to sidestep Dynamo's rejection of empty maps / unsupported nested scalars.
/** @internal */
export const enc = <T>(value: T): string | undefined =>
  value === undefined ? undefined : JSON.stringify(value);

/** @internal */
export const dec = (raw: string | null | undefined): Json | undefined =>
  raw === undefined || raw === null ? undefined : JSON.parse(raw);

const decError = (raw: string | undefined): FlowError | undefined =>
  decodeFlowError(dec(raw), "dynamodb: error");

// The document client types every attribute as `any`; each read checks what it got.
/** @internal */
export const str = (item: DocItem, name: string): string => {
  const v = item[name];
  if (v !== String(v)) throw new Error(`dynamodb: attribute "${name}" is not a string`);
  return v;
};

/** @internal */
export const num = (item: DocItem, name: string): number => {
  const v = item[name];
  if (!Number.isFinite(v)) throw new Error(`dynamodb: attribute "${name}" is not a number`);
  return v;
};

const optStr = (item: DocItem, name: string): string | undefined =>
  item[name] === undefined || item[name] === null ? undefined : str(item, name);

const optNum = (item: DocItem, name: string): number | undefined =>
  item[name] === undefined || item[name] === null ? undefined : num(item, name);

/** @internal */
export const runStatusOf = (item: DocItem): RunRow["status"] =>
  decodeOneOf(RUN_STATUSES, str(item, "status"), "dynamodb: run status");

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
  priority?: number;
  seq: number;
}

export const parseRun = (item: DocItem): RunItem => ({
  id: str(item, "id"),
  name: str(item, "name"),
  version: num(item, "version"),
  status: runStatusOf(item),
  input: optStr(item, "input"),
  output: optStr(item, "output"),
  error: optStr(item, "error"),
  attempts: num(item, "attempts"),
  idempotencyKey: optStr(item, "idempotencyKey"),
  tags: decodeTags(item.tags, "dynamodb: run tags"),
  parentRunId: optStr(item, "parentRunId"),
  parentCursorKey: optStr(item, "parentCursorKey"),
  depth: optNum(item, "depth"),
  createdAt: optStr(item, "createdAt"),
  priority: optNum(item, "priority"),
  seq: num(item, "seq"),
});

export const mapRun = (r: RunItem): RunRow => ({
  id: r.id,
  name: r.name,
  version: r.version,
  status: r.status,
  input: dec(r.input),
  attempts: r.attempts,
  output: dec(r.output),
  error: decError(r.error),
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
  cronInput?: string | null;
  overlap: "allow" | "skip";
  nextRunAt: number;
  lastRunAt?: number;
}

export const parseCron = (item: DocItem): CronItem => ({
  cronName: str(item, "cronName"),
  schedule: str(item, "schedule"),
  flowName: str(item, "flowName"),
  flowVersion: num(item, "flowVersion"),
  cronInput: optStr(item, "cronInput"),
  overlap: decodeOneOf(CRON_OVERLAPS, str(item, "overlap"), "dynamodb: cron overlap"),
  nextRunAt: num(item, "nextRunAt"),
  lastRunAt: optNum(item, "lastRunAt"),
});

export interface StepItem {
  status: StepOutcome["status"];
  result?: string;
  error?: string;
  attempts: number;
  call?: string;
}

export const parseStep = (item: DocItem): StepItem => ({
  status: decodeOneOf(STEP_STATUSES, str(item, "status"), "dynamodb: step status"),
  result: optStr(item, "result"),
  error: optStr(item, "error"),
  attempts: num(item, "attempts"),
  call: optStr(item, "call"),
});

export const mapStep = (r: StepItem): StepOutcome => ({
  status: r.status,
  result: dec(r.result),
  error: decError(r.error),
  attempts: r.attempts,
  call: r.call,
});

export interface JobItem {
  runId: string;
  runAt: number;
  version?: number;
  leaseExpires?: number;
}

export const parseJob = (item: DocItem): JobItem => ({
  runId: str(item, "runId"),
  runAt: num(item, "runAt"),
  version: optNum(item, "version"),
  leaseExpires: optNum(item, "leaseExpires"),
});

export interface TimerItem {
  runId: string;
  fireAt: number;
}

export const parseTimer = (item: DocItem): TimerItem => ({
  runId: str(item, "runId"),
  fireAt: num(item, "fireAt"),
});
