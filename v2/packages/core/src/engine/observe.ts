import type { SuspendStatus } from "#types";

/** Granularity of the durable event log. `lifecycle` = run-level only; `all` adds step events. */
export type EventLevel = "all" | "lifecycle" | "off";

/** The durable event kinds the sink records — run lifecycle transitions, per-step completion, and `ctx.log`. */
export type EventType =
  | "run.started"
  | "run.completed"
  | "run.failed"
  | "run.suspended"
  | "step.finished"
  | "run.log";

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

/**
 * One completed step, as a tracing span. `traceId` is stable per run and `spanId` is derived from
 * the step's positional cursor, so a step replayed across ticks/crashes keeps the SAME ids — the
 * exporter dedups naturally. Emitted only when a step actually executes (a memoized replay is silent).
 * A workflow that fans out links traces by run id: a child run's `traceId` derives from its own id,
 * so an exporter joins parent→child on the spawned run id.
 */
export interface Span {
  runId: string;
  /** 32-hex-char trace id (W3C traceparent shape), stable for the whole run. */
  traceId: string;
  /** 16-hex-char span id, derived from the step's cursor — identical on every replay of that step. */
  spanId: string;
  /** The step name (`ctx.step`'s first arg). */
  name: string;
  startedAt: Date;
  endedAt: Date;
  /** Present when the step failed permanently (the error that propagated). */
  error?: { code: string; message: string };
}

/** A span sink — wire it to `@opentelemetry/api` (or any tracer) to export durable step spans. */
export interface Tracer {
  span(span: Span): void;
}

/** In-process telemetry callbacks — cheap, non-durable, for OTel/StatsD wiring. */
export interface Metrics {
  runStarted?(runId: string): void;
  runSettled?(runId: string, status: "done" | "failed"): void;
  runSuspended?(runId: string, status: SuspendStatus): void;
  stepFinished?(runId: string, cursorKey: string): void;
  tickError?(err: unknown): void;
}

/** Observability wiring passed to the worker. All optional — omit for zero overhead. */
export interface ObserveOpts {
  sink?: EventSink;
  level?: EventLevel;
  metrics?: Metrics;
  /** Durable step-span sink, for OTel/tracing export. See {@link Span}. */
  tracer?: Tracer;
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
  readonly tracer?: Tracer;
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
    tracer: opts?.tracer,
  };
};
