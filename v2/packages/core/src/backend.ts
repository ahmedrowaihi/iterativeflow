/**
 * The backend-author SPI (`@iterativeflow/core/backend`). Everything needed to IMPLEMENT a
 * substrate — the four port interfaces, the shapes a durable write moves, and the shared
 * helpers that keep a backend correct (the local wakeup bus, the status constants a reconciler
 * and health snapshot depend on). App code never imports from here; it uses the main entry.
 */

export type { Store, StartResult } from "#ports/store";
export type { Queue, ClaimOpts, EnqueueOpts, Lease } from "#ports/queue";
export type { Timer, TimerDueOpts } from "#ports/timer";
export type { Wakeup } from "#ports/wakeup";
export type { Backend, Outbox, SpawnRequest, EnqueueRequest, TimerRequest } from "#ports/outbox";

export type {
  RunRow,
  RunSnapshot,
  RunSpec,
  RunStatus,
  RunFilter,
  Page,
  RunPage,
  StepStatus,
  StepOutcome,
  StepCheckpoint,
  SuspendStatus,
  TerminalOutcome,
  DeliveredSignal,
  FlowError,
  CronRow,
  CronSpec,
} from "#types";

export type { EventSink, EventType, FlowEvent } from "#engine/observe";

export { createLocalWakeup } from "#local-wakeup";
export { RUN_STATUSES } from "#types";
export {
  TERMINAL_STATUSES,
  ACTIVE_STATUSES,
  RECONCILABLE_STATUSES,
  isTerminal,
  isRunStatus,
  statusList,
  zeroRunStats,
} from "#status";
export { newId } from "#id";
export type { IdGen } from "#id";
