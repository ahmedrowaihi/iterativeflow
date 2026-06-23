import type { CronSpec } from "./types";

/** The slice of the engine a drain/dispatch needs — just the run entrypoint. */
export interface RunHandler {
  handleRun(runId: string): Promise<void>;
}

/** Inputs a {@link Dispatcher} needs to begin driving runs and crons. */
export interface DispatcherStartOpts {
  /** The stateless claim → replay → run-to-suspend → persist cycle for one run. */
  handleRun: (runId: string) => Promise<void>;
  /** Internal (reconcile/retention) + user crons to fire on schedule. */
  crons: CronSpec[];
  /** Run a cron body within the engine's tracing wrapper. */
  runCron: (name: string, fn: () => Promise<void>) => Promise<void>;
  /** Flow identities this process registered — routes jobs to a matching worker. */
  flows: ReadonlyArray<{ name: string; version: number }>;
}

/**
 * Drives runs: pulls work and calls `handleRun`. A resident dispatcher owns a
 * poll loop; a serverless dispatcher no-ops `start` and lets the host invoke
 * `engine.handleRun` per request.
 */
export interface Dispatcher {
  /** Begin dispatching. Resident impls start a worker; serverless impls no-op. */
  start(opts: DispatcherStartOpts): Promise<void>;
  /** Drain in-flight runs and stop dispatching. Idempotent. */
  stop(): Promise<void>;
  /** Whether the dispatcher is actively consuming — surfaced by `health()`. */
  running(): boolean;
}
