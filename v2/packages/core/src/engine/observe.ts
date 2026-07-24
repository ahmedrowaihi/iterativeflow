/** Granularity of the durable event log. `lifecycle` = run-level only; `all` adds step events. */
export type EventLevel = "all" | "lifecycle" | "off";

export type EventType =
  | "run.started"
  | "run.completed"
  | "run.failed"
  | "run.suspended"
  | "step.finished";

/** One durable audit-log entry — the dashboard timeline reads these. */
export interface FlowEvent {
  runId: string;
  type: EventType;
  at: Date;
  data?: unknown;
}

/** Where durable events are written. A Postgres sink persists them; the default is off. */
export interface EventSink {
  record(event: FlowEvent): void | Promise<void>;
}

/** In-process telemetry callbacks — cheap, non-durable, for OTel/StatsD wiring. */
export interface Metrics {
  runStarted?(runId: string): void;
  runSettled?(runId: string, status: "done" | "failed"): void;
  runSuspended?(runId: string, status: string): void;
  stepFinished?(runId: string, cursorKey: string): void;
  tickError?(err: unknown): void;
}

/** Observability wiring passed to the worker. All optional — omit for zero overhead. */
export interface ObserveOpts {
  sink?: EventSink;
  level?: EventLevel;
  metrics?: Metrics;
}

const LIFECYCLE = new Set<EventType>([
  "run.started",
  "run.completed",
  "run.failed",
  "run.suspended",
]);

/** @internal */
export interface Observer {
  event(type: EventType, runId: string, at: Date, data?: unknown): Promise<void>;
  readonly metrics: Metrics;
}

export const makeObserver = (opts?: ObserveOpts): Observer => {
  const level = opts?.level ?? "off";
  const sink = opts?.sink;
  return {
    async event(type, runId, at, data) {
      if (!sink || level === "off") return;
      if (level === "lifecycle" && !LIFECYCLE.has(type)) return;
      await sink.record({ runId, type, at, data });
    },
    metrics: opts?.metrics ?? {},
  };
};
