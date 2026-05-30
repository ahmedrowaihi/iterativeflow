import type { Logger } from "../../engine/types";
import type { WorkflowDb } from "../db";
import type { EnqueueOpts } from "../types";

/** @internal */
export const asTx = (tx: unknown): WorkflowDb => tx as WorkflowDb;

/** Transaction-scoped enqueue. The runtime supplies the active `tx` (or root db). */
export type TxEnqueue = (tx: WorkflowDb, runId: string, opts?: EnqueueOpts) => Promise<void>;

/** @internal */
export interface DrizzleStorageOpts {
  db: WorkflowDb;
  logger: Logger;
  enqueue: TxEnqueue;
}

/** @internal */
export const noopEnqueue: TxEnqueue = async () => {};

/** @internal */
export const RESUMABLE = ["pending", "sleeping", "awaiting_signal", "retrying", "running"] as const;

/** @internal */
export const TERMINAL = ["done", "failed", "canceled"] as const;
