import type { AtomicStorage, Storage } from "../types";
import { claimRun } from "./claim";
import { invokeBudget } from "./invoke-budget";
import { notifyTerminal } from "./notify";
import { buildOps } from "./ops";
import { findChildRun, listChildren, listRuns, loadOutput, loadRunDetail } from "./queries";
import { pruneEvents, pruneRuns } from "./prune";
import { reenqueueOrphans } from "./reconcile";
import { getSchemaVersion } from "./schema-version";
import { armOrConsumeSignal, deliverSignal } from "./signals";
import { asTx, type DrizzleStorageOpts } from "./types";

export type { TxEnqueue, DrizzleStorageOpts } from "./types";
export { noopEnqueue } from "./types";

/**
 * Compose the Drizzle-backed {@link Storage}. The `db` / `enqueue` pair is
 * shared with sub-modules; each sub-module exposes the slice of behaviour
 * it owns (claim, signals, queries, prune, reconcile, schema-version,
 * invoke-budget, notify). `transaction(fn)` rebuilds ops over the inner
 * `tx` so writes inside the closure are atomic.
 */
export const createDrizzleStorage = (opt: DrizzleStorageOpts): Storage => {
  const { db, enqueue, logger } = opt;

  const root = buildOps(db, enqueue);

  return {
    ...root.ops,

    async transaction(fn) {
      return db.transaction(async (tx) => {
        const txDb = asTx(tx);
        const inner = buildOps(txDb, enqueue);
        const atomic: AtomicStorage = {
          ...inner.ops,
          lockRun: inner.lockRun,
          enqueue: inner.enqueue,
        };
        return fn(atomic);
      });
    },

    claimRun: claimRun(db),
    deliverSignal: deliverSignal(db, enqueue),
    armOrConsumeSignal: armOrConsumeSignal(db),
    loadRunDetail: loadRunDetail(db),
    loadOutput: loadOutput(db),
    listRuns: listRuns(db),
    findChildRun: findChildRun(db),
    listChildren: listChildren(db),
    invokeBudget: invokeBudget(db),
    getSchemaVersion: getSchemaVersion(db),
    notifyTerminal: notifyTerminal(db, enqueue),
    reenqueueOrphans: reenqueueOrphans(db, enqueue, logger),
    pruneEvents: pruneEvents(db),
    pruneRuns: pruneRuns(db),
  };
};
