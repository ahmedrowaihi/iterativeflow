import type { Dispatcher } from "../../engine/scheduler";

/**
 * A {@link Dispatcher} with no resident poll loop. Serverless hosts drive runs
 * by calling `engine.handleRun(runId)` from an HTTP route and `drainDueWakes`
 * from a scheduled trigger, so `listen()` starts nothing. Provided so a
 * serverless engine can still call `listen()`/`stop()` uniformly.
 */
export const createServerlessDispatcher = (): Dispatcher => {
  let started = false;
  return {
    async start() {
      started = true;
    },
    async stop() {
      started = false;
    },
    running: () => started,
  };
};
