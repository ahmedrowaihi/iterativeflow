import type { BackoffPolicy } from "../util/backoff";
import type { Duration } from "../util/duration";
import type { StandardSchemaV1 } from "../util/standard-schema";
import type { RunStatus } from "../storage/schema";

export interface StepOpts {
  retries?: number;
  backoff?: BackoffPolicy;
  baseBackoffMs?: number;
  capBackoffMs?: number;
  classify?: (err: Error) => "transient" | "permanent";
  timeoutMs?: number;
}

export interface HookOpts<T> {
  schema?: StandardSchemaV1<unknown, T>;
  timeout?: Duration;
}

export interface WorkflowContext {
  readonly runId: string;
  readonly attempt: number;
  step<T>(name: string, fn: () => Promise<T> | T, opts?: StepOpts): Promise<T>;
  sleep(duration: Duration): Promise<void>;
  hook<T = unknown>(name: string, opts?: HookOpts<T>): Promise<T>;
  log(message: string, payload?: Record<string, unknown>): void;
}

export interface DefineWorkflowOpts<I, O> {
  name: string;
  version?: number;
  input?: StandardSchemaV1<unknown, I>;
  run: (ctx: WorkflowContext, input: I) => Promise<O> | O;
}

export interface CronSpec {
  name: string;
  /** Standard 5-field cron pattern (minute hour dom month dow), UTC. */
  schedule: string;
  run: () => Promise<unknown> | unknown;
  /** Catch up runs missed during downtime (ms). Default 0 — no backfill. */
  backfillPeriod?: number;
}

export interface StartOpts {
  idempotencyKey?: string;
  priority?: number;
  delay?: Duration;
}

export interface WorkflowHandle<I, O> {
  readonly name: string;
  readonly version: number;
  start(input: I, opts?: StartOpts): Promise<{ runId: string; status: RunStatus }>;
  output(runId: string): Promise<O | undefined>;
}

export interface Logger {
  debug(message: string, payload?: Record<string, unknown>): void;
  info(message: string, payload?: Record<string, unknown>): void;
  warn(message: string, payload?: Record<string, unknown>): void;
  error(err: Error, payload?: Record<string, unknown>): void;
}

export type {
  ArmResult,
  AtomicStorage,
  EnqueueOpts,
  RunDetail,
  RunSnapshot,
  SignalResult,
  Storage,
  StorageOps,
} from "../storage/types";
