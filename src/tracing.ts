import { SpanStatusCode, trace } from "@opentelemetry/api";

const TRACER_NAME = "@aws-vod/workflow";
const TRACER_VERSION = "0.1.0";

const tracer = trace.getTracer(TRACER_NAME, TRACER_VERSION);

export type TaskKind = "workflow" | "cron";

export const withTaskSpan = (kind: TaskKind, id: string, fn: () => Promise<void>): Promise<void> =>
  tracer.startActiveSpan(`workflow.${kind}.${id}`, async (span) => {
    span.setAttribute("messaging.system", "graphile-worker");
    span.setAttribute("messaging.destination.name", id);
    span.setAttribute("workflow.kind", kind);
    span.setAttribute("workflow.id", id);
    try {
      await fn();
    } catch (err) {
      span.recordException(err as Error);
      span.setStatus({
        code: SpanStatusCode.ERROR,
        message: err instanceof Error ? err.message : String(err),
      });
      throw err;
    } finally {
      span.end();
    }
  });
