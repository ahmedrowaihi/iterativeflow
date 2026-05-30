import { enforcePayloadCap } from "../util/payload-cap";
import { validate } from "../util/standard-schema";
import type { FlowRegistry } from "./registry";
import type { Logger, MetricsRecorder, SignalDeliveryResult, SignalIssue, Storage } from "./types";

interface Deps {
  storage: Storage;
  registry: FlowRegistry;
  logger: Logger;
  metrics: MetricsRecorder;
  maxSignalPayloadBytes?: number;
}

/**
 * `engine.signal` routing: enforce the payload size cap, look up the
 * builder-declared schema (if any) for the run's `(flow, version)`, validate
 * the payload at *delivery* time, and only persist via `storage.deliverSignal`
 * once validation passes. Schema lookup is skipped when the run does not exist.
 *
 * @internal
 */
export const createSignalRouter =
  (deps: Deps) =>
  async (runId: string, signalName: string, payload?: unknown): Promise<SignalDeliveryResult> => {
    enforcePayloadCap(`Signal "${signalName}" payload`, payload, deps.maxSignalPayloadBytes);
    const run = await deps.storage.loadRun(runId);
    if (run) {
      const schema = deps.registry.signalSchema(run.name, run.version, signalName);
      if (schema) {
        const parsed = await validate(schema, payload);
        if (parsed.issues) {
          const issues: SignalIssue[] = parsed.issues.map((i) => ({
            path: i.path?.map((p) =>
              typeof p === "object" && p !== null && "key" in p
                ? ((p as { key: PropertyKey }).key as string | number)
                : (p as string | number),
            ),
            message: i.message,
          }));
          const result: SignalDeliveryResult = { kind: "invalid_payload", issues };
          deps.metrics.signalDelivered?.({ signal: signalName, kind: result.kind });
          return result;
        }
      }
    }
    const result = await deps.storage.deliverSignal(runId, signalName, payload);
    deps.metrics.signalDelivered?.({ signal: signalName, kind: result.kind });
    return result;
  };
