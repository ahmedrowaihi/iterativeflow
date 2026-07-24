/**
 * The app API — authoring flows and operating the engine. Implementing a new backend? Use
 * `@iterativeflow/core/backend` instead: the port interfaces + helpers.
 *
 * @packageDocumentation
 */
export type {
  DeliveredSignal,
  FlowError,
  Page,
  RunFilter,
  RunPage,
  RunRow,
  RunSnapshot,
  RunStatus,
  StepOutcome,
  StepStatus,
} from "#types";
export { RUN_STATUSES } from "#types";
export { isRunStatus } from "#status";
/** The backend `createEngine` runs on; construct one with a backend package (or the SPI). */
export type { Backend } from "#ports/outbox";

export { defineFlow, registry, type, validateInput, validateSignal } from "#engine/flow";
export type {
  AnyFlow,
  Flow,
  FlowPolicy,
  FlowRegistry,
  InputSchema,
  InvokeOutputs,
  InvokeSpec,
  NoSignals,
  SignalMap,
  SignalSchema,
  SignalSchemas,
} from "#engine/flow";
export { builder, FlowBuilder } from "#engine/builder";
export { systemClock } from "#engine/context";
export type { Clock, Ctx, StepArg, StepPolicy } from "#engine/context";

export { createEngine } from "#engine/engine";
export type { Engine, EngineOpts, RunLoopOpts } from "#engine/engine";
export {
  submit,
  submitMany,
  signalRun,
  cancelRun,
  retryRun,
  result,
  reconcile,
  drainTimers,
  tickOnce,
  serverlessTick,
} from "#engine/worker";
export type {
  OnDuplicate,
  RunHandle,
  RunResult,
  SubmitItem,
  SubmitOpts,
  SweepResult,
  TickOnceOpts,
} from "#engine/worker";
export { runTick, defaultRetry } from "#engine/executor";
export type { DriftPolicy, RetryPolicy, TickResult, TickOpts } from "#engine/executor";

export { parseCron, nextCronAfter } from "#engine/cron";
export { cronTag, registerCron, runDueCrons } from "#engine/schedule";
export type { CronDef } from "#engine/schedule";

export {
  SleepSignal,
  AwaitChildSignal,
  AwaitSignalSignal,
  DuplicateRunError,
  FlowDriftError,
  StepFailedError,
  StepTimeoutError,
  isControlSignal,
} from "#engine/signals";
export type { ControlSignal } from "#engine/signals";

export type {
  EventLevel,
  EventSink,
  EventType,
  FlowEvent,
  Metrics,
  ObserveOpts,
} from "#engine/observe";

export { newId } from "#id";
export type { IdGen } from "#id";
