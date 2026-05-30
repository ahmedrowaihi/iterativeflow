import { toMs, type Duration } from "../util/duration";
import type { Logger } from "./types";

/** @internal */
export const validateLogger = (logger: Logger): void => {
  for (const m of ["debug", "info", "warn", "error"] as const) {
    if (typeof logger[m] !== "function") {
      throw new Error(`logger.${m} must be a function`);
    }
  }
};

/** @internal */
export const validateRetention = (
  retention: { runsOlderThan?: Duration; eventsOlderThan?: Duration } | undefined,
): void => {
  if (!retention) return;
  if (retention.runsOlderThan !== undefined) toMs(retention.runsOlderThan);
  if (retention.eventsOlderThan !== undefined) toMs(retention.eventsOlderThan);
};

/** @internal */
export const warnIfPoolUndersized = (
  poolMax: number | undefined,
  concurrency: number,
  logger: Logger,
): void => {
  if (typeof poolMax === "number" && concurrency > poolMax) {
    logger.warn("flow.config.pool_too_small", { concurrency, poolMax });
  }
};

/**
 * Warn if the reconciler's `running` stuck threshold is smaller than the
 * fallback step timeout. A live step running between those bounds is
 * indistinguishable from a crashed process — the reconciler would
 * resurrect it, producing two concurrent attempts of the same run.
 *
 * @internal
 */
export const warnIfStuckShorterThanStepTimeout = (
  runningStuckMs: number,
  defaultStepTimeoutMs: number | undefined,
  logger: Logger,
): void => {
  if (defaultStepTimeoutMs === undefined) return;
  if (runningStuckMs < defaultStepTimeoutMs) {
    logger.warn("flow.config.stuck_shorter_than_step_timeout", {
      runningStuckMs,
      defaultStepTimeoutMs,
      hint: "raise runningStuckMs >= defaultStepTimeoutMs (plus headroom) so the reconciler doesn't resurrect a still-running step",
    });
  }
};
