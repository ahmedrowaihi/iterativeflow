import type { WorkflowDb } from "./db";
import type {
  EventType,
  FlowError,
  RunRow,
  RunStatus,
  SignalRow,
  StepRow,
  StepStatus,
  TimerRow,
} from "./schema";

export interface EnqueueOpts {
  runAt?: Date;
  priority?: number;
}

/** Single validation failure for a signal payload, shaped after Standard Schema's `Issue`. */
export interface SignalIssue {
  /** Path into the payload where the failure occurred. */
  readonly path?: ReadonlyArray<string | number>;
  /** Human-readable description. */
  readonly message: string;
}

/** Outcome of `engine.signal(...)`. Branch on `kind` to decide HTTP response. */
export type SignalDeliveryResult =
  | { kind: "delivered"; cursorKey: string }
  | { kind: "buffered"; cursorKey: string }
  | { kind: "duplicate" }
  | { kind: "expired"; cursorKey: string }
  | { kind: "invalid_payload"; issues: ReadonlyArray<SignalIssue> };

export type ArmResult = { kind: "consumed"; payload: unknown; row: SignalRow } | { kind: "armed" };

/** Outcome of `engine.retry(...)`. Branch on `kind` to decide HTTP response. */
export type RetryResult =
  | { kind: "queued" }
  | { kind: "missing" }
  | { kind: "not_failed"; status: RunStatus };

export interface ClaimedRun {
  run: RunRow;
  snapshot: RunSnapshot;
  resumed: boolean;
}

export type ClaimResult =
  | { kind: "claimed"; claim: ClaimedRun }
  | { kind: "missing" }
  | { kind: "terminal"; status: RunStatus }
  | { kind: "lost" };

/** Full snapshot returned by `engine.status(runId)`. */
export interface RunDetail {
  /** Row from `workflow.runs`. */
  run: RunRow;
  /** Rows from `workflow.steps` for this run. */
  steps: StepRow[];
  /** Rows from `workflow.timers` for this run. */
  timers: TimerRow[];
  /** Rows from `workflow.signals` for this run. */
  signals: SignalRow[];
}

/** Cursor-keyed maps loaded once at claim time and read by `RuntimeFlowContext`. */
export interface RunSnapshot {
  /** Step rows by cursor key. */
  steps: Map<string, StepRow>;
  /** Timer (sleep) rows by cursor key. */
  timers: Map<string, TimerRow>;
  /** Signal rows by cursor key. */
  signals: Map<string, SignalRow>;
}

/** Filters for `engine.listRuns(...)`. All optional; absent fields don't constrain. */
export interface ListRunsOpts {
  /** Match `runs.name`. */
  name?: string;
  /** Match any of these statuses. */
  status?: ReadonlyArray<RunStatus>;
  /** Match runs whose `tags` array contains this tag. */
  tag?: string;
  /** Only runs created on or after this instant. */
  since?: Date;
  /** Only runs created on or before this instant. */
  until?: Date;
  /** Max rows. Default 50; throws if greater than 500. */
  limit?: number;
  /** Keyset cursor from a prior page's `next`. */
  cursor?: { createdAt: Date; id: string };
}

/** One page returned by `engine.listRuns(...)`. */
export interface ListRunsPage {
  /** Rows for this page (most recent first). */
  runs: RunRow[];
  /** Cursor for the next page, or undefined when this is the last. */
  next?: { createdAt: Date; id: string };
}

/** Everything `Storage.startRun` needs to insert a run, record its started event, and enqueue. */
export interface StartRunSpec {
  name: string;
  version: number;
  input: unknown;
  idempotencyKey?: string;
  tags?: ReadonlyArray<string>;
  parentRunId?: string;
  parentCursorKey?: string;
  /** First-run delay, already resolved to an absolute instant. */
  runAt?: Date;
  /** Queue priority passed through to `enqueue`. */
  priority?: number;
}

export interface StorageOps {
  createRun(opt: {
    name: string;
    version: number;
    input: unknown;
    idempotencyKey?: string;
    tags?: ReadonlyArray<string>;
    parentRunId?: string;
    parentCursorKey?: string;
  }): Promise<{ runId: string; status: RunStatus; created: boolean }>;
  loadRun(runId: string): Promise<RunRow | undefined>;
  loadSnapshot(runId: string): Promise<RunSnapshot>;
  markRunning(runId: string): Promise<void>;
  markSleeping(runId: string): Promise<void>;
  markAwaitingSignal(runId: string): Promise<void>;
  markRetrying(runId: string): Promise<void>;
  markCompleted(runId: string, output: unknown): Promise<void>;
  markFailed(runId: string, error: FlowError): Promise<void>;
  markCanceled(runId: string, reason?: string): Promise<void>;

  loadStep(runId: string, cursorKey: string): Promise<StepRow | undefined>;
  startStep(runId: string, cursorKey: string, attempts: number): Promise<void>;
  finishStep(opt: {
    runId: string;
    cursorKey: string;
    status: StepStatus;
    result?: unknown;
    error?: FlowError;
    attempts: number;
  }): Promise<StepRow>;

  loadTimer(runId: string, cursorKey: string): Promise<TimerRow | undefined>;
  createTimer(runId: string, cursorKey: string, fireAt: Date): Promise<void>;
  fireTimer(runId: string, cursorKey: string): Promise<TimerRow>;

  loadSignal(runId: string, cursorKey: string): Promise<SignalRow | undefined>;
  preDeliverSignal(runId: string, cursorKey: string, payload: unknown): Promise<boolean>;

  recordEvent(opt: {
    runId: string;
    type: EventType;
    cursorKey?: string;
    payload?: unknown;
  }): Promise<void>;
}

export interface AtomicStorage extends StorageOps {
  lockRun(runId: string): Promise<RunRow | undefined>;
  enqueue(runId: string, opt?: EnqueueOpts): Promise<void>;
}

export interface Storage extends StorageOps {
  transaction<T>(fn: (tx: AtomicStorage) => Promise<T>): Promise<T>;
  /** Insert a run, record its `started` event, and enqueue — atomically. Joins `tx` when supplied (caller owns the commit), else opens its own transaction. */
  startRun(
    spec: StartRunSpec,
    tx?: WorkflowDb,
  ): Promise<{ runId: string; status: RunStatus; created: boolean }>;
  claimRun(runId: string): Promise<ClaimResult>;
  deliverSignal(runId: string, signalName: string, payload: unknown): Promise<SignalDeliveryResult>;
  armOrConsumeSignal(runId: string, cursorKey: string, expiresAt?: Date): Promise<ArmResult>;
  loadRunDetail(runId: string): Promise<RunDetail | undefined>;
  loadOutput(runId: string): Promise<unknown>;
  findChildRun(parentRunId: string, parentCursorKey: string): Promise<RunRow | undefined>;
  listChildren(parentRunId: string): Promise<RunRow[]>;
  invokeBudget(runId: string): Promise<{ depth: number; childCount: number }>;
  getSchemaVersion(): Promise<number>;
  listRuns(opt: ListRunsOpts): Promise<ListRunsPage>;
  notifyTerminal(runId: string): Promise<void>;
  reenqueueOrphans(opt: {
    olderThan: Date;
    runningStuckOlderThan: Date;
    batchSize?: number;
  }): Promise<number>;
  pruneEvents(opt: { olderThan: Date; batchSize?: number }): Promise<number>;
  pruneRuns(opt: {
    olderThan: Date;
    status?: ReadonlyArray<RunStatus>;
    batchSize?: number;
  }): Promise<number>;
  retryRun(runId: string): Promise<RetryResult>;
}
