import {
  ACTIVE_RUN_STATUSES as ACTIVE_STATUSES,
  RUN_STATUSES,
  type RunStatus,
} from "../../../storage/run-statuses";

export { ACTIVE_STATUSES, RUN_STATUSES, type RunStatus };

export interface CappedJson {
  preview: string;
  truncated: boolean;
  size: number;
}

export interface FlowErrorLite {
  code: string;
  message: string;
  stack?: string;
}

export interface RunListItem {
  id: string;
  name: string;
  version: number;
  status: RunStatus;
  attempts: number;
  tags?: string[];
  createdAt: string;
  startedAt?: string | null;
  completedAt?: string | null;
  error?: FlowErrorLite | null;
}

export interface RunFull extends RunListItem {
  parentRunId?: string | null;
  idempotencyKey?: string | null;
  input?: CappedJson | null;
  output?: CappedJson | null;
}

export interface StepRow {
  cursorKey: string;
  status: RunStatus;
  attempts: number;
  startedAt?: string | null;
  completedAt?: string | null;
  result?: CappedJson | null;
  error?: FlowErrorLite | null;
}

export interface TimerRow {
  cursorKey: string;
  fireAt: string;
  firedAt?: string | null;
}

export interface SignalRow {
  cursorKey: string;
  name: string;
  delivered: boolean;
  createdAt: string;
  deliveredAt?: string | null;
  expiresAt?: string | null;
  payload?: CappedJson | null;
}

export interface RunDetail {
  run: RunFull;
  steps: StepRow[];
  timers: TimerRow[];
  signals: SignalRow[];
}

export type SignalDeliveryResult =
  | { kind: "delivered"; cursorKey: string }
  | { kind: "buffered"; cursorKey: string }
  | { kind: "duplicate" }
  | { kind: "expired"; cursorKey: string }
  | { kind: "invalid_payload"; issues: ReadonlyArray<{ path: string; message: string }> };

export interface RunsPage {
  runs: RunListItem[];
  next: { createdAt: string; id: string } | null;
}

export interface CronRow {
  name: string;
  schedule: string;
  timezone: string;
  overlap: string;
}

export interface HealthReport {
  db: boolean;
  worker: boolean;
  listen: boolean;
}

export interface CronTriggerResult {
  ok: boolean;
  result: CappedJson | null;
}

export interface Filters {
  name: string;
  status: RunStatus | "";
  tag: string;
  since: string;
  until: string;
}
