import type { Logger, MetricsRecorder } from "../engine/types";

/** @internal */
export const wrapMetrics = (metrics: MetricsRecorder, logger: Logger): MetricsRecorder => {
  const failed = new Set<string>();
  const out: Record<string, (p: unknown) => void> = {};
  for (const key of Object.keys(metrics)) {
    const orig = (metrics as Record<string, unknown>)[key];
    if (typeof orig !== "function") continue;
    const bound = (orig as (p: unknown) => void).bind(metrics);
    out[key] = (payload: unknown) => {
      try {
        bound(payload);
      } catch (err) {
        if (!failed.has(key)) {
          failed.add(key);
          logger.warn("flow.metrics.threw", {
            method: key,
            message: err instanceof Error ? err.message : String(err),
          });
        }
      }
    };
  }
  return out as MetricsRecorder;
};
