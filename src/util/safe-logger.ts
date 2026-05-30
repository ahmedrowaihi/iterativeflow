import type { Logger } from "../engine/types";

type LoggerMethod = "debug" | "info" | "warn" | "error";

const stderrOnce = (msg: string, err: unknown): void => {
  try {
    console.error(
      `[iterativeflow] ${msg}:`,
      err instanceof Error ? `${err.name}: ${err.message}` : String(err),
    );
  } catch {
    // last resort — swallow
  }
};

/** @internal */
export const wrapLogger = (logger: Logger): Logger => {
  const failed = new Set<LoggerMethod>();
  const guard = <K extends LoggerMethod>(method: K): Logger[K] => {
    const bound = logger[method].bind(logger);
    return ((...args: unknown[]) => {
      try {
        (bound as (...a: unknown[]) => void)(...args);
      } catch (err) {
        if (!failed.has(method)) {
          failed.add(method);
          stderrOnce(`logger.${method} threw — further logger.${method} calls suppressed`, err);
        }
      }
    }) as Logger[K];
  };
  return {
    debug: guard("debug"),
    info: guard("info"),
    warn: guard("warn"),
    error: guard("error"),
  };
};
