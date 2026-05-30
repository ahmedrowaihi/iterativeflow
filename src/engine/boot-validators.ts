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
