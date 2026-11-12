/**
 * The backend-author SPI (`@iterativeflow/core/backend`). Everything needed to IMPLEMENT a
 * substrate — the four port interfaces, the shapes a durable write moves, and the shared
 * helpers that keep a backend correct (the local wakeup bus, the status constants a reconciler
 * and health snapshot depend on). App code never imports from here; it uses the main entry.
 */

export type { Store, StartResult } from "#ports/store";
export type {
  EnqueueFields,
  Queue,
  ClaimOpts,
  EnqueueOpts,
  EnqueueRequest,
  Lease,
  QueueDepth,
} from "#ports/queue";
export { queueDepthOf, distinctEnqueues, enqueueFields, shardAdmits } from "#ports/queue";
export type { Timer, TimerDueOpts } from "#ports/timer";
export type { Wakeup } from "#ports/wakeup";
export type { Backend, Outbox, SpawnRequest, TimerRequest } from "#ports/outbox";
export { FAN_OUT_CHUNK } from "#ports/outbox";

export type {
  RunRow,
  RunSnapshot,
  RunSpec,
  RunStatus,
  RunFilter,
  PurgeFilter,
  TerminalStatus,
  Page,
  RunPage,
  StepStatus,
  StepOutcome,
  StepCheckpoint,
  SuspendStatus,
  TerminalOutcome,
  DeliveredSignal,
  FlowError,
  CronOverlap,
  CronRow,
  CronSpec,
} from "#types";

export type { EventSink, EventType, FlowEvent } from "#engine/observe";
export { EVENT_TYPES, isEventType } from "#engine/observe";

export { createLocalWakeup } from "#local-wakeup";
export { CRON_OVERLAPS, RUN_STATUSES, STEP_STATUSES } from "#types";
export {
  TERMINAL_STATUSES,
  NON_SUCCESS_TERMINAL_STATUSES,
  ACTIVE_STATUSES,
  isTerminal,
  isRunStatus,
  resetsAttempts,
  zeroRunStats,
} from "#status";
export { decodeFlowError, decodeOneOf, decodeTags, durable, isJsonString } from "#json";
export type { Json } from "#json";
export { newId } from "#id";
export type { IdGen } from "#id";
export {
  cancelSelection,
  listSelection,
  purgeSelection,
  retrySelection,
  runMatcher,
  runSelectionSql,
  sqlStatusTuple,
} from "#selection";
export type { RunSelection, SelectableRun, SelectionSqlOpts } from "#selection";
export { isOrphaned, orphanedRunsSql } from "#orphan";
export type { OrphanView, OrphanRun, OrphanSqlOpts } from "#orphan";
export { assertSqlIdentifier } from "#sql-name";
