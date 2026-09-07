import { sha256hex } from "#engine/sha256";
import type { SuspendStatus } from "#types";

const hashHex = (seed: string, bytes: number): string => sha256hex(seed).slice(0, bytes * 2);

/** @internal */
export const traceIdOf = (runId: string): string => hashHex(runId, 16);
/** @internal */
export const spanIdOf = (runId: string, cursorKey: string): string =>
  hashHex(`${runId}:${cursorKey}`, 8);

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
  traceId: string;
  spanId: string;
  name: string;
  startedAt: Date;
  endedAt: Date;
  error?: { code: string; message: string };
}

/** A span sink — wire it to `@opentelemetry/api` (or any tracer) to export durable step spans. */
export interface Tracer {
  span(span: Span): void;
}

/** Which flow a metric belongs to, so a callback can label without reading the store. */
export interface FlowLabel {
  name: string;
  version: number;
}

/** In-process telemetry callbacks — cheap, non-durable, for OTel/StatsD wiring. Durations are
 *  wall-clock milliseconds, absent when the source instant was never recorded. */
export interface Metrics {
  runStarted?(runId: string, flow: FlowLabel): void;
  runSettled?(
    runId: string,
    status: "done" | "failed",
    flow: FlowLabel,
    extra?: { durationMs?: number; errorCode?: string },
  ): void;
  runSuspended?(runId: string, status: SuspendStatus, flow: FlowLabel): void;
  redeployParked?(runId: string, reason: "unknown_flow" | "flow_drift"): void;
  stepFinished?(runId: string, cursorKey: string, extra?: { durationMs?: number }): void;
  tickError?(err: unknown): void;
}

/** Observability wiring passed to the worker. All optional — omit for zero overhead. */
export interface ObserveOpts {
  sink?: EventSink;
  level?: EventLevel;
  metrics?: Metrics;
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
  records(type: EventType): boolean;
  readonly metrics: Metrics;
  readonly tracer?: Tracer;
}

export const makeObserver = (opts?: ObserveOpts): Observer => {
  const level = opts?.level ?? "off";
  const sink = opts?.sink;
  const records = (type: EventType): boolean =>
    !!sink && level !== "off" && !(level === "lifecycle" && !LIFECYCLE.has(type));
  const metrics = opts?.metrics ?? {};
  const tracer = opts?.tracer;
  return {
    async event(type, runId, at, data) {
      // Observability is never load-bearing: a throwing sink must not reach the executor, where it
      // would be caught as a flow error and skip the parent-wake and ack that follow a terminal write.
      if (!records(type)) return;
      try {
        await sink!.record({ runId, type, at, data });
      } catch (e) {
        metrics.tickError?.(e);
      }
    },
    records,
    metrics,
    tracer: tracer && {
      span: (span) => {
        try {
          tracer.span(span);
        } catch (e) {
          metrics.tickError?.(e);
        }
      },
    },
  };
};
