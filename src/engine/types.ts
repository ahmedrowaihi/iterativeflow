import type { BackoffPolicy } from "../util/backoff";
import type { Duration } from "../util/duration";
import type { StandardSchemaV1 } from "../util/standard-schema";
import type { RunStatus } from "../storage/schema";
import type { SignalDeliveryResult } from "../storage/types";

/** Argument passed to a step body. */
export interface StepArg<I = unknown> {
  /** Channel input from the previous builder step. `undefined` for `ctx.step(...)` invocations. */
  readonly input: I;
  /**
   * Cancellation/timeout signal for the step body. Propagates the configured
   * `StepOpts.timeoutMs` and `engine.cancel(runId)` to fetch / pg / undici
   * / abortable APIs.
   */
  readonly signal: AbortSignal;
  /** 1-indexed retry attempt for THIS step (1 = first call, 2 = first retry, ...). */
  readonly attempt: number;
}

/** Options for a single `ctx.step(...)` invocation. */
export interface StepOpts {
  /** Number of additional attempts after the first failure (default `3`, total attempts = retries + 1). */
  retries?: number;
  /** Backoff curve for retries. */
  backoff?: BackoffPolicy;
  /** Base delay for the backoff curve (ms). */
  baseBackoffMs?: number;
  /** Maximum delay between attempts (ms). */
  capBackoffMs?: number;
  /** Decides whether an error is retryable. `"permanent"` skips remaining retries. */
  classify?: (err: Error) => "transient" | "permanent";
  /** Hard timeout for a single attempt (ms). Triggers `signal.abort()` and a retry. */
  timeoutMs?: number;
}

/** Options for a single `ctx.signal(...)` await. */
export interface SignalOpts<T> {
  /** Standard Schema validator applied to the delivered payload. */
  schema?: StandardSchemaV1<unknown, T>;
  /** Suspend the run with `SIGNAL_TIMEOUT` after this duration without delivery. */
  timeout?: Duration;
}

/** Runtime context passed to every flow body. */
export interface FlowContext {
  /** The current run's UUID. */
  readonly runId: string;
  /** 1-indexed run-level attempt counter (1 = first execution). */
  readonly attempt: number;
  /** Execute a step exactly once across replays. Result is memoized in storage. */
  step<T>(name: string, fn: (arg: StepArg) => Promise<T> | T, opts?: StepOpts): Promise<T>;
  /** Suspend the run until `duration` has elapsed. */
  sleep(duration: Duration): Promise<void>;
  /** Suspend the run until an external `engine.signal(runId, name, ...)` arrives. */
  signal<T = unknown>(name: string, opts?: SignalOpts<T>): Promise<T>;
  /** Start a child flow and await its terminal output. */
  invoke<I, O>(handle: FlowHandle<I, O>, input: I, opts?: InvokeOpts): Promise<O>;
  /** Structured logger scoped to the current `runId`. */
  log(message: string, payload?: Record<string, unknown>): void;
}

/** Options for `ctx.invoke(...)`. */
export interface InvokeOpts {
  /** Tags propagated to the child run; not inherited automatically. */
  tags?: ReadonlyArray<string>;
  /** Idempotency key for the child run. Scoped under the child's (name, version). */
  idempotencyKey?: string;
}

/** Hand-written flow definition. Equivalent to `flow().build()` for the simple `(ctx, input) => out` case. */
export interface DefineFlowOpts<I, O> {
  /** Flow name. */
  name: string;
  /** Schema version; defaults to `1`. Bump when you reshape the flow body. */
  version?: number;
  /** Standard Schema validator applied to the run input. */
  input?: StandardSchemaV1<unknown, I>;
  /** Flow body. Receives `FlowContext` and the validated input. */
  body: (ctx: FlowContext, input: I) => Promise<O> | O;
}

/** Specification for a cron task registered via `engine.defineCron(...)`. */
export interface CronSpec {
  /** Cron task name; must not collide with engine-reserved names. */
  name: string;
  /** 5-field cron pattern (minute hour dom month dow). UTC unless `timezone` is set. */
  schedule: string;
  /** IANA timezone (e.g. "America/Los_Angeles"). Default UTC. */
  timezone?: string;
  /** Overlap policy: `skip` (default) prevents concurrent runs via advisory lock, `allow` runs in parallel. */
  overlap?: "skip" | "allow";
  /** Jitter (ms) applied to each scheduled fire to spread load. */
  jitterMs?: number;
  /** Catch up runs missed during downtime (ms). Default 0 — no backfill. */
  backfillPeriod?: number;
  /** Task body. Errors are re-thrown so graphile-worker retries. */
  run: () => Promise<unknown> | unknown;
}

/** Options for `handle.start(...)`. */
export interface StartOpts {
  /** Idempotency key scoped under `(flowName, flowVersion)`. Repeated starts return the original run. */
  idempotencyKey?: string;
  /** Graphile-worker priority (-32768..32767, lower = sooner). */
  priority?: number;
  /** Delay the first run by this duration. */
  delay?: Duration;
  /** Free-form tags for filtering via `engine.listRuns({ tag })`. */
  tags?: ReadonlyArray<string>;
}

/**
 * Wait condition for {@link FlowHandle.wait}. Either a step name (first
 * occurrence — cursor key matches the step name exactly) or a signal name
 * (first occurrence — cursor key is `signal:<name>`).
 */
export type WaitUntil = { step: string } | { signal: string };

/** Handle returned by `engine.register(...)` — the typed entry point to a flow. */
export interface FlowHandle<I, O> {
  /** Registered flow name. */
  readonly name: string;
  /** Registered flow version. */
  readonly version: number;
  /** Insert a row and enqueue. The worker (started by `engine.listen()`) consumes it. */
  start(input: I, opts?: StartOpts): Promise<{ runId: string; status: RunStatus }>;
  /** Resolved output if the run is `done`, else `undefined`. */
  output(runId: string): Promise<O | undefined>;
  /**
   * Block until the run reaches a terminal state (done/failed/canceled).
   * Subscribes via Postgres LISTEN with a row-poll fallback.
   */
  result(runId: string, opt?: { timeoutMs?: number }): Promise<O>;
  /**
   * Block until a specific in-flow event occurs:
   * - `{ step: "fetch" }` — resolves when the *first occurrence* of `ctx.step("fetch")`
   *   finishes with status `ok` or `failed_terminal`.
   * - `{ signal: "approve" }` — resolves when the *first occurrence* of
   *   `ctx.signal("approve")` is delivered (or buffered for a future arm).
   *
   * Backed by `LISTEN flow_progress`. Rejects on `timeoutMs`. Does NOT reject
   * automatically on terminal — callers who want either-or should compose with
   * `handle.result()` via `Promise.race`.
   */
  wait(runId: string, opts: { until: WaitUntil; timeoutMs?: number }): Promise<void>;
}

/** Structured logger contract. Bring your own (pino, winston, console, ...). */
export interface Logger {
  /** Verbose / trace events. */
  debug(message: string, payload?: Record<string, unknown>): void;
  /** Informational events. */
  info(message: string, payload?: Record<string, unknown>): void;
  /** Recoverable degradations. */
  warn(message: string, payload?: Record<string, unknown>): void;
  /** Errors. First argument is the `Error`, payload is structured context. */
  error(err: Error, payload?: Record<string, unknown>): void;
}

/** Why a run suspended — reported via `MetricsRecorder.runSuspended` and the `suspended` event payload. */
export type SuspendReason = "sleep" | "awaiting_signal" | "step_retry";

/**
 * Optional engine telemetry. Every method is optional; the engine wraps each
 * call defensively so a throwing recorder cannot crash a run.
 */
export interface MetricsRecorder {
  /** Fired once when a fresh run row is inserted. */
  runStarted?(opt: { name: string; version: number }): void;
  /** Fired when a run reaches `done`. `durationMs` is wall-clock from claim to terminal. */
  runCompleted?(opt: { name: string; version: number; durationMs: number }): void;
  /** Fired when a run reaches `failed`. `code` is the persisted `FlowError.code`. */
  runFailed?(opt: { name: string; version: number; code: string }): void;
  /** Fired on each `FlowSuspend` thrown by the body (sleep, await_signal, step_retry). */
  runSuspended?(opt: { name: string; version: number; reason: SuspendReason }): void;
  /** Fired after every step attempt (ok, retry, or terminal). */
  stepFinished?(opt: {
    name: string;
    version: number;
    step: string;
    status: "ok" | "failed_retry" | "failed_terminal";
    durationMs: number;
  }): void;
  /** Fired per `engine.signal(...)` call. `kind` is the {@link SignalDeliveryResult} variant. */
  signalDelivered?(opt: { signal: string; kind: SignalDeliveryResult["kind"] }): void;
  /** Fired per reconciler tick. */
  reconcilerSweep?(opt: { scanned: number; reEnqueued: number }): void;
}

/** Snapshot returned by `engine.health()`. `ok` is `db && worker`. */
export interface HealthReport {
  /** Composite: `db && worker`. */
  ok: boolean;
  /** Latest `SELECT 1` succeeded. */
  db: boolean;
  /** Graphile-worker is running. */
  worker: boolean;
  /** `true` while the engine is subscribed to `LISTEN flow_terminal`. */
  listen: boolean;
  /** When `engine.listen()` last succeeded. */
  startedAt?: Date;
}

export type {
  ArmResult,
  AtomicStorage,
  ClaimResult,
  ClaimedRun,
  EnqueueOpts,
  ListRunsOpts,
  ListRunsPage,
  RunDetail,
  RunSnapshot,
  SignalDeliveryResult,
  SignalIssue,
  Storage,
  StorageOps,
} from "../storage/types";
