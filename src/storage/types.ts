import type {
  EventType,
  HookRow,
  RunRow,
  RunStatus,
  StepRow,
  StepStatus,
  TimerRow,
  WorkflowError,
} from "./schema";

export interface EnqueueOpts {
  runAt?: Date;
  priority?: number;
}

export type SignalResult =
  | { kind: "delivered"; hookKey: string }
  | { kind: "buffered"; hookKey: string }
  | { kind: "duplicate" };

export type ArmResult = { kind: "consumed"; payload: unknown } | { kind: "armed" };

export interface RunDetail {
  run: RunRow;
  steps: StepRow[];
  timers: TimerRow[];
  hooks: HookRow[];
}

export interface RunSnapshot {
  steps: Map<string, StepRow>;
  timers: Map<string, TimerRow>;
  hooks: Map<string, HookRow>;
}

export interface StorageOps {
  createRun(opt: {
    name: string;
    version: number;
    input: unknown;
    idempotencyKey?: string;
  }): Promise<{ runId: string; status: RunStatus; created: boolean }>;
  loadRun(runId: string): Promise<RunRow | undefined>;
  loadSnapshot(runId: string): Promise<RunSnapshot>;
  markRunning(runId: string): Promise<void>;
  markSleeping(runId: string): Promise<void>;
  markWaiting(runId: string): Promise<void>;
  markCompleted(runId: string, output: unknown): Promise<void>;
  markFailed(runId: string, error: WorkflowError): Promise<void>;
  markCanceled(runId: string, reason?: string): Promise<void>;

  loadStep(runId: string, stepKey: string): Promise<StepRow | undefined>;
  startStep(runId: string, stepKey: string, attempts: number): Promise<void>;
  finishStep(opt: {
    runId: string;
    stepKey: string;
    status: StepStatus;
    result?: unknown;
    error?: WorkflowError;
    attempts: number;
  }): Promise<void>;

  loadTimer(runId: string, stepKey: string): Promise<TimerRow | undefined>;
  createTimer(runId: string, stepKey: string, fireAt: Date): Promise<void>;
  fireTimer(runId: string, stepKey: string): Promise<void>;

  loadHook(runId: string, hookKey: string): Promise<HookRow | undefined>;
  preDeliverHook(runId: string, hookKey: string, payload: unknown): Promise<boolean>;

  recordEvent(opt: {
    runId: string;
    type: EventType;
    stepKey?: string;
    payload?: unknown;
  }): Promise<void>;
}

export interface AtomicStorage extends StorageOps {
  lockRun(runId: string): Promise<RunRow | undefined>;
  enqueue(runId: string, opt?: EnqueueOpts): Promise<void>;
}

export interface Storage extends StorageOps {
  transaction<T>(fn: (tx: AtomicStorage) => Promise<T>): Promise<T>;
  signalHook(runId: string, hookName: string, payload: unknown): Promise<SignalResult>;
  armOrConsumeHook(runId: string, hookKey: string, expiresAt?: Date): Promise<ArmResult>;
  loadRunDetail(runId: string): Promise<RunDetail | undefined>;
  loadOutput(runId: string): Promise<unknown>;
  reenqueueOrphans(opt: {
    olderThan: Date;
    runningStuckOlderThan?: Date;
    batchSize?: number;
  }): Promise<number>;
  pruneEvents(opt: { olderThan: Date; batchSize?: number }): Promise<number>;
  pruneRuns(opt: {
    olderThan: Date;
    status?: ReadonlyArray<RunStatus>;
    batchSize?: number;
  }): Promise<number>;
}
