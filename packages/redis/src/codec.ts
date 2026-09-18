import {
  type CronRow,
  type CronSpec,
  type DeliveredSignal,
  type RunRow,
  type RunSpec,
  type RunStatus,
  type StepOutcome,
  CRON_OVERLAPS,
  RUN_STATUSES,
  STEP_STATUSES,
  decodeFlowError,
  decodeOneOf,
  decodeTags,
} from "@iterativeflow/core/backend";
import { RUN } from "#keys";

const num = (v: string | undefined, d = 0): number => (v === undefined ? d : Number(v));

const runStatus = (v: string | undefined): RunStatus =>
  decodeOneOf(RUN_STATUSES, String(v), "redis: run status");

/** Flat `HSET` field/value pairs for a fresh run (`status: pending`, `attempts: 0`,
 *  `joinRemaining: 0`); `seq` is assigned atomically by the creating Lua (`INCR`), not here. */
export const runFields = (spec: RunSpec, runId: string): string[] => {
  const f = [
    RUN.id,
    runId,
    RUN.name,
    spec.name,
    RUN.version,
    String(spec.version),
    RUN.status,
    "pending",
    RUN.input,
    JSON.stringify(spec.input ?? null),
    RUN.attempts,
    "0",
    RUN.depth,
    String(spec.depth ?? 0),
    RUN.createdAt,
    String((spec.createdAt ?? new Date()).getTime()),
    RUN.joinRemaining,
    "0",
  ];
  if (spec.idempotencyKey !== undefined) f.push(RUN.idempotencyKey, spec.idempotencyKey);
  if (spec.tags !== undefined) f.push(RUN.tags, JSON.stringify(spec.tags));
  if (spec.parentRunId !== undefined) f.push(RUN.parentRunId, spec.parentRunId);
  if (spec.parentCursorKey !== undefined) f.push(RUN.parentCursorKey, spec.parentCursorKey);
  if (spec.priority !== undefined) f.push(RUN.priority, String(spec.priority));
  return f;
};

/** Decode a `run:{id}` HGETALL into a {@link RunRow}. Returns undefined for a missing (empty) run. */
export const toRunRow = (h: Record<string, string>): RunRow | undefined => {
  if (!h[RUN.id]) return undefined;
  const output = h[RUN.output];
  const error = h[RUN.error];
  const tags = h[RUN.tags];
  return {
    id: h[RUN.id],
    name: h[RUN.name],
    version: num(h[RUN.version]),
    status: runStatus(h[RUN.status]),
    input: JSON.parse(h[RUN.input] ?? "null") ?? undefined,
    attempts: num(h[RUN.attempts]),
    output: output === undefined ? undefined : JSON.parse(output),
    error: error === undefined ? undefined : decodeFlowError(JSON.parse(error), "redis: run error"),
    idempotencyKey: h[RUN.idempotencyKey],
    tags: tags === undefined ? undefined : decodeTags(JSON.parse(tags), "redis: run tags"),
    parentRunId: h[RUN.parentRunId],
    parentCursorKey: h[RUN.parentCursorKey],
    depth: num(h[RUN.depth]),
    createdAt: h[RUN.createdAt] ? new Date(num(h[RUN.createdAt])) : undefined,
  };
};

export const idemIdentity = (name: string, version: number, key: string): string =>
  JSON.stringify([name, version, key]);

export const encodeStep = (s: StepOutcome): string => JSON.stringify(s);
export const decodeStep = (v: string): StepOutcome => {
  const s = JSON.parse(v);
  return {
    status: decodeOneOf(STEP_STATUSES, String(s.status), "redis: step status"),
    result: s.result,
    error: decodeFlowError(s.error, "redis: step error"),
    attempts: Number(s.attempts),
    call: s.call === undefined ? undefined : String(s.call),
  };
};

export const encodeSignal = (s: DeliveredSignal): string => JSON.stringify(s);
export const decodeSignal = (v: string): DeliveredSignal => {
  const s = JSON.parse(v);
  return { id: String(s.id), name: String(s.name), payload: s.payload };
};

export const encodeCron = (c: CronRow): string => JSON.stringify(c);
export const decodeCron = (v: string): CronRow => {
  const c = JSON.parse(v);
  return {
    name: String(c.name),
    schedule: String(c.schedule),
    flowName: String(c.flowName),
    flowVersion: Number(c.flowVersion),
    input: c.input,
    overlap: decodeOneOf(CRON_OVERLAPS, String(c.overlap), "redis: cron overlap"),
    nextRunAt: new Date(c.nextRunAt),
    lastRunAt: c.lastRunAt === undefined ? undefined : new Date(c.lastRunAt),
  };
};
export const cronRowFromSpec = (spec: CronSpec, nextRunAt: Date): CronRow => ({
  name: spec.name,
  schedule: spec.schedule,
  flowName: spec.flowName,
  flowVersion: spec.flowVersion,
  input: spec.input,
  overlap: spec.overlap ?? "allow",
  nextRunAt,
});
