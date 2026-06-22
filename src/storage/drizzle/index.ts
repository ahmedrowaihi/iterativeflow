import { eq } from "drizzle-orm";
import { flowTables as defaultTables } from "../schema";
import type { AtomicStorage, Storage } from "../types";
import type { WorkflowDb } from "../db";
import type { EnqueueRun } from "./types";
import { claimRun } from "./claim";
import { invokeBudget } from "./invoke-budget";
import { notifyTerminal } from "./notify";
import { buildOps } from "./ops";
import { pruneEvents, pruneRuns } from "./prune";
import { findChildRun, listChildren, listRuns, loadOutput, loadRunDetail } from "./queries";
import { reenqueueOrphans } from "./reconcile";
import { retryRun } from "./retry";
import { getSchemaVersion } from "./schema-version";
import { armOrConsumeSignal, deliverSignal } from "./signals";
import type { DrizzleStorageOpts, StorageSliceDeps } from "./types";

export type { TxEnqueue, DrizzleStorageOpts, InternalTables, StorageSliceDeps } from "./types";
export { noopEnqueue } from "./types";

/**
 * Compose the Drizzle-backed {@link Storage}. `transaction(fn)` rebuilds ops
 * over the inner `tx` so writes inside the closure commit atomically.
 */
export const createDrizzleStorage = (opt: DrizzleStorageOpts): Storage => {
  const { db, enqueue, logger } = opt;
  const tables = opt.tables ?? defaultTables;

  const enqueueRun: EnqueueRun = async (tx: WorkflowDb, runId, opts) => {
    const [r] = await tx
      .select({ name: tables.runs.name, version: tables.runs.version })
      .from(tables.runs)
      .where(eq(tables.runs.id, runId))
      .limit(1);
    if (!r) throw new Error(`enqueue: run ${runId} not found`);
    await enqueue(tx, { runId, name: r.name, version: r.version }, opts);
  };

  const deps: StorageSliceDeps = { db, tables, enqueue: enqueueRun, logger };
  const root = buildOps({ db, tables, enqueue: enqueueRun });

  return {
    ...root.ops,

    async transaction(fn) {
      return db.transaction(async (tx) => {
        const inner = buildOps({ db: tx, tables, enqueue: enqueueRun });
        const atomic: AtomicStorage = {
          ...inner.ops,
          lockRun: inner.lockRun,
          enqueue: inner.enqueue,
        };
        return fn(atomic);
      });
    },

    claimRun: claimRun(deps),
    deliverSignal: deliverSignal(deps),
    armOrConsumeSignal: armOrConsumeSignal(deps),
    loadRunDetail: loadRunDetail(deps),
    loadOutput: loadOutput(deps),
    listRuns: listRuns(deps),
    findChildRun: findChildRun(deps),
    listChildren: listChildren(deps),
    invokeBudget: invokeBudget(deps),
    getSchemaVersion: getSchemaVersion(deps),
    notifyTerminal: notifyTerminal(deps),
    reenqueueOrphans: reenqueueOrphans(deps),
    pruneEvents: pruneEvents(deps),
    pruneRuns: pruneRuns(deps),
    retryRun: retryRun(deps),
  };
};
