import type { Logger } from "../../engine/types";
import type { WorkflowDb } from "../db";
import type { events, runs, signals, steps, timers } from "../schema";
import type { EnqueueOpts } from "../types";

/** Transaction-scoped enqueue. The runtime supplies the active `tx` (or root db). */
export type TxEnqueue = (tx: WorkflowDb, runId: string, opts?: EnqueueOpts) => Promise<void>;

/** @internal */
export interface InternalTables {
  runs: typeof runs;
  steps: typeof steps;
  signals: typeof signals;
  timers: typeof timers;
  events: typeof events;
}

/** @internal */
export interface DrizzleStorageOpts {
  db: WorkflowDb;
  logger: Logger;
  enqueue: TxEnqueue;
  /** Override the internal tables — only needed when consumers customize names. */
  tables?: InternalTables;
}

/**
 * Shared shape every Storage sub-module receives. Sub-modules destructure
 * only what they need; the consistent input shape catches arg-order
 * mistakes at type-check time.
 *
 * @internal
 */
export interface StorageSliceDeps {
  db: WorkflowDb;
  tables: InternalTables;
  enqueue: TxEnqueue;
  logger: Logger;
}

/** @internal */
export const noopEnqueue: TxEnqueue = async () => {};

/** @internal */
export const RESUMABLE = ["pending", "sleeping", "awaiting_signal", "retrying", "running"] as const;

/** @internal */
export const TERMINAL = ["done", "failed", "canceled"] as const;
