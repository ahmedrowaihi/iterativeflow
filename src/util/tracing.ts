import { SpanStatusCode, trace } from "@opentelemetry/api";

const TRACER_NAME = "iterativeflow";

const tracer = trace.getTracer(TRACER_NAME);

export type TaskKind = "flow" | "cron";

export const withTaskSpan = (kind: TaskKind, id: string, fn: () => Promise<void>): Promise<void> =>
  tracer.startActiveSpan(`flow.${kind}.${id}`, async (span) => {
    span.setAttribute("messaging.system", "graphile-worker");
    span.setAttribute("messaging.destination.name", id);
    span.setAttribute("flow.kind", kind);
    span.setAttribute("flow.id", id);
    try {
      await fn();
    } catch (err) {
      try {
        span.recordException(err as Error);
        span.setStatus({
          code: SpanStatusCode.ERROR,
          message: err instanceof Error ? err.message : String(err),
        });
      } catch {
        // tracer disconnected — never let observability break the runtime
      }
      throw err;
    } finally {
      span.end();
    }
  });
