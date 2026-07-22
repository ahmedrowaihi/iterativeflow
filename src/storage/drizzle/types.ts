import type { Logger } from "../../engine/types";
import type { WorkflowDb } from "../db";
import type { events, runs, signals, steps, timers } from "../schema";
import type { EnqueueOpts } from "../types";

/** A single enqueue target: the run's identity plus its scheduling opts. */
export interface EnqueueJob {
  job: { runId: string; name: string; version: number };
  opts?: EnqueueOpts;
}

/**
 * Transaction-scoped enqueue routed by flow identity. The runtime supplies the
 * active `tx` (or root db). `name`/`version` select the per-flow graphile task
 * identifier so only a worker that registered the flow can claim the job.
 *
 * `many` is an optional bulk path: adapters that can enqueue N jobs in one
 * round-trip implement it; callers fall back to looping the single form when
 * it's absent.
 */
export interface TxEnqueue {
  (
    tx: WorkflowDb,
    job: { runId: string; name: string; version: number },
    opts?: EnqueueOpts,
  ): Promise<void>;
  many?(tx: WorkflowDb, jobs: ReadonlyArray<EnqueueJob>): Promise<void>;
}

/**
 * Storage-internal enqueue keyed by `runId` alone. {@link createDrizzleStorage}
 * resolves `name`/`version` from `tables.runs` once, then delegates to the raw
 * {@link TxEnqueue}. Call sites that only hold a `runId` use this.
 *
 * @internal
 */
export type EnqueueRun = (tx: WorkflowDb, runId: string, opts?: EnqueueOpts) => Promise<void>;

/**
 * Normalize `db.execute()` result. `node-postgres` returns `{ rows }`;
 * `postgres-js` and some drizzle 1.x drivers return an array directly.
 *
 * @internal
 */
export const rowsOf = <T = Record<string, unknown>>(result: unknown): T[] => {
  const r = result as { rows?: T[] } | T[];
  return Array.isArray(r) ? r : (r.rows ?? []);
};

/** @internal */
export interface InternalTables {
  runs: typeof runs;
  steps: typeof steps;
  signals: typeof signals;
  timers: typeof timers;
  events: typeof events;
}

/**
 * Audit-event granularity. `workflow.events` is never read on the resume path,
 * so any setting preserves durability — it only trades observability for fewer
 * DB round-trips. `"lifecycle"` drops the high-volume per-step events
 * (`step_started`/`step_ok`/…), keeping run-level ones; `"off"` records none.
 *
 * @internal
 */
export type EventPolicy = "all" | "lifecycle" | "off";

/**
 * Whether a `workflow.events` row of `type` should be written under `policy`.
 * Step events are the per-step hot path; run-level events are low-volume.
 *
 * @internal
 */
export const shouldRecordEvent = (type: string, policy: EventPolicy): boolean =>
  policy === "all" ? true : policy === "off" ? false : !type.startsWith("step_");

/** Round-trip-reduction knobs shared across the storage slices. @internal */
export interface ObsConfig {
  /** Emit `pg_notify` on state changes for cross-process wakeups. Off = poll-only (RDS Proxy). */
  notify: boolean;
  /** Audit-event granularity. */
  events: EventPolicy;
}

/** @internal */
export interface DrizzleStorageOpts {
  db: WorkflowDb;
  logger: Logger;
  enqueue: TxEnqueue;
  /** Override the internal tables — only needed when consumers customize names. */
  tables?: InternalTables;
  /** Round-trip-reduction knobs. Defaults: `notify: true`, `events: "all"`. */
  obs?: Partial<ObsConfig>;
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
  enqueue: EnqueueRun;
  logger: Logger;
  obs: ObsConfig;
}

/** @internal */
export const noopEnqueue: TxEnqueue = async () => {};

/** @internal */
export const RESUMABLE = ["pending", "sleeping", "awaiting_signal", "retrying", "running"] as const;

/** @internal */
export const TERMINAL = ["done", "failed", "canceled"] as const;
