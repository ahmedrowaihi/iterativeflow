import type {
  CronRow,
  CronSpec,
  DeliveredSignal,
  FlowError,
  RunRow,
  RunSpec,
  RunStatus,
  StepOutcome,
} from "@iterativeflow/core/backend";
import { RUN } from "#keys";

const num = (v: string | undefined, d = 0): number => (v === undefined ? d : Number(v));
const json = <T>(v: string | undefined): T | undefined =>
  v === undefined ? undefined : (JSON.parse(v) as T);

/** Flat `HSET` fields for a fresh run (`status: pending`, `attempts: 0`, `joinRemaining: 0`);
 *  `seq` is assigned atomically by the creating Lua (`INCR`), not here. */
export const runFields = (spec: RunSpec, runId: string): Record<string, string> => {
  const f: Record<string, string> = {
    [RUN.id]: runId,
    [RUN.name]: spec.name,
    [RUN.version]: String(spec.version),
    [RUN.status]: "pending",
    [RUN.input]: JSON.stringify(spec.input ?? null),
    [RUN.attempts]: "0",
    [RUN.depth]: String(spec.depth ?? 0),
    [RUN.createdAt]: String((spec.createdAt ?? new Date()).getTime()),
    [RUN.joinRemaining]: "0",
  };
  if (spec.idempotencyKey !== undefined) f[RUN.idempotencyKey] = spec.idempotencyKey;
  if (spec.tags !== undefined) f[RUN.tags] = JSON.stringify(spec.tags);
  if (spec.parentRunId !== undefined) f[RUN.parentRunId] = spec.parentRunId;
  if (spec.parentCursorKey !== undefined) f[RUN.parentCursorKey] = spec.parentCursorKey;
  return f;
};

/** Decode a `run:{id}` HGETALL into a {@link RunRow}. Returns undefined for a missing (empty) run. */
export const toRunRow = (h: Record<string, string>): RunRow | undefined => {
  if (!h[RUN.id]) return undefined;
  return {
    id: h[RUN.id],
    name: h[RUN.name],
    version: num(h[RUN.version]),
    status: h[RUN.status] as RunStatus,
    input: json(h[RUN.input]) ?? undefined,
    attempts: num(h[RUN.attempts]),
    output: json(h[RUN.output]),
    error: json<FlowError>(h[RUN.error]),
    idempotencyKey: h[RUN.idempotencyKey],
    tags: json<string[]>(h[RUN.tags]),
    parentRunId: h[RUN.parentRunId],
    parentCursorKey: h[RUN.parentCursorKey],
    depth: num(h[RUN.depth]),
    createdAt: h[RUN.createdAt] ? new Date(num(h[RUN.createdAt])) : undefined,
  };
};

export const idemIdentity = (name: string, version: number, key: string): string =>
  JSON.stringify([name, version, key]);

export const encodeStep = (s: StepOutcome): string => JSON.stringify(s);
export const decodeStep = (v: string): StepOutcome => JSON.parse(v) as StepOutcome;

export const encodeSignal = (s: DeliveredSignal): string => JSON.stringify(s);
export const decodeSignal = (v: string): DeliveredSignal => JSON.parse(v) as DeliveredSignal;

export const encodeCron = (c: CronRow): string => JSON.stringify(c);
export const decodeCron = (v: string): CronRow => JSON.parse(v) as CronRow;
export const cronRowFromSpec = (spec: CronSpec, nextRunAt: Date): CronRow => ({
  name: spec.name,
  schedule: spec.schedule,
  flowName: spec.flowName,
  flowVersion: spec.flowVersion,
  input: spec.input,
  overlap: spec.overlap ?? "allow",
  nextRunAt,
});
