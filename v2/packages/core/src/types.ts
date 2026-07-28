/** The canonical run states, in lifecycle order. `RunStatus` is derived from this — one source. */
export const RUN_STATUSES = [
  "pending",
  "running",
  "sleeping",
  "awaiting_signal",
  "awaiting_child",
  "retrying",
  "done",
  "failed",
  "canceled",
] as const;

export type RunStatus = (typeof RUN_STATUSES)[number];

/** The non-terminal states a running run can be parked in, each with its own wake path. */
export type SuspendStatus = "sleeping" | "awaiting_signal" | "awaiting_child" | "retrying";

/** What a run does when its flow body drifted under it: park (recoverable) or fail (terminal). */
export type DriftPolicy = "park" | "fail";

/** Structured error persisted on failed runs/steps. */
export interface FlowError {
  code: string;
  message: string;
  stack?: string;
  cause?: string;
}

/** Everything needed to start (or idempotently re-find) a run. */
export interface RunSpec {
  name: string;
  version: number;
  input: unknown;
  /** Idempotency key scoped under `(name, version)`. Repeats return the original run. */
  idempotencyKey?: string;
  tags?: readonly string[];
  parentRunId?: string;
  parentCursorKey?: string;
  /** Distance from a top-level submit: 0 for a direct submit, parent+1 for a child. Bounds `ctx.invoke` recursion. */
  depth?: number;
  /** Creation instant, stamped from the engine clock at submit/spawn. Drives retention. Backends
   *  default it to their own clock when a direct `startRun` omits it. */
  createdAt?: Date;
}

/** A checkpointed step is always a success — a step failure fails the run, not the memo, so only
 *  successful steps are ever written (their existence IS the success marker). */
export type StepStatus = "ok";

/** The durable memo of a completed step — the one thing that must survive a crash. */
export interface StepOutcome {
  status: StepStatus;
  result?: unknown;
  error?: FlowError;
  /** 1-indexed attempt count this outcome was reached on. */
  attempts: number;
  /**
   * The `kind:label` of the `ctx` call that produced this memo (e.g. `step:charge`, `signal:approve`).
   * On replay the executor compares it to the call now issued at this cursor; a mismatch means the
   * flow body was reordered/refactored under a live run (drift). Absent on memos written before the
   * drift guard existed, in which case the check is skipped.
   */
  shape?: string;
}

/** A step checkpoint request — the single durable write per step. */
export interface StepCheckpoint extends StepOutcome {
  runId: string;
  cursorKey: string;
}

/** Row shape of a run. */
export interface RunRow {
  id: string;
  name: string;
  version: number;
  status: RunStatus;
  input: unknown;
  attempts: number;
  output?: unknown;
  error?: FlowError;
  idempotencyKey?: string;
  tags?: readonly string[];
  parentRunId?: string;
  parentCursorKey?: string;
  depth?: number;
  createdAt?: Date;
}

/** A durable signal delivered to a run's inbox, awaiting consumption by a `ctx.signal` wait. */
export interface DeliveredSignal {
  /** Inbox row id — the handle a consuming checkpoint passes to `Outbox.consumeSignals`. */
  id: string;
  name: string;
  payload: unknown;
}

/** What `loadRun` returns: the run, the memo of completed steps, and the pending signal inbox. */
export interface RunSnapshot {
  run: RunRow;
  /** Completed steps by cursor key — read on resume so memoized steps short-circuit. */
  steps: ReadonlyMap<string, StepOutcome>;
  /** Signals delivered but not yet consumed — a `ctx.signal(name)` wait drains a matching one. */
  signals: readonly DeliveredSignal[];
}

/** Filter for {@link Store.listRuns}. `status` accepts one or several states. */
export interface RunFilter {
  status?: RunStatus | readonly RunStatus[];
  name?: string;
  tag?: string;
}

/** A page request — `cursor` is the opaque token returned by the previous page. */
export interface Page {
  limit: number;
  cursor?: string;
}

/** A page of runs (newest first) plus the cursor for the next page (absent when exhausted). */
export interface RunPage {
  runs: readonly RunRow[];
  cursor?: string;
}

/** A registered recurring schedule. `nextRunAt` is the next fire instant (UTC). */
export interface CronRow {
  name: string;
  schedule: string;
  flowName: string;
  flowVersion: number;
  input: unknown;
  overlap: "allow" | "skip";
  nextRunAt: Date;
  lastRunAt?: Date;
}

/** Register/upsert payload for a cron — `nextRunAt` is computed by the engine from `schedule`. */
export interface CronSpec {
  name: string;
  schedule: string;
  flowName: string;
  flowVersion: number;
  input?: unknown;
  overlap?: "allow" | "skip";
  nextRunAt: Date;
}

/** A terminal transition. */
export type TerminalOutcome =
  | { status: "done"; output: unknown }
  | { status: "failed"; error: FlowError }
  | { status: "canceled"; error?: FlowError };
