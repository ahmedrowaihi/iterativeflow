import type { Logger } from "./types";

/**
 * Used when `EngineOpts.logger` isn't provided. `debug`/`info` stay silent;
 * `warn`/`error` pipe to stderr so boot validators and runtime errors are
 * at least visible. A consumer who genuinely wants silence passes their
 * own no-op logger.
 *
 * @internal
 */
export const fallbackLogger: Logger = {
  debug: () => undefined,
  info: () => undefined,
  warn: (msg, payload) => {
    try {
      console.warn(`[iterativeflow] ${msg}`, payload ?? {});
    } catch {
      // ignore stderr failures
    }
  },
  error: (err, payload) => {
    try {
      console.error(`[iterativeflow]`, err, payload ?? {});
    } catch {
      // ignore stderr failures
    }
  },
};

/** Minimal {@link Logger} that prints structured payloads to `console`. */
export const consoleLogger = (): Logger => ({
  debug: (m, p) => console.debug(m, p ?? {}),
  info: (m, p) => console.info(m, p ?? {}),
  warn: (m, p) => console.warn(m, p ?? {}),
  error: (e, p) => console.error(e, p ?? {}),
});
