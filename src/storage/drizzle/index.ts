import { randomUUID } from "node:crypto";
import { and, eq, or, sql } from "drizzle-orm";
import type { RunStatus } from "../schema";
import { flowTables as defaultTables } from "../schema";
import type { AtomicStorage, StartRunSpec, Storage } from "../types";
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

export type {
  TxEnqueue,
  EnqueueJob,
  DrizzleStorageOpts,
  InternalTables,
  StorageSliceDeps,
} from "./types";
export { noopEnqueue } from "./types";

// Rows per bulk statement in a batch start. The runs insert binds the most
// columns (~9), so this keeps every statement under Postgres' 65535 bind-param
// ceiling with wide margin; a larger batch just runs as more statements in the
// same transaction, so it stays atomic and unbounded.
const RUN_INSERT_CHUNK = 1000;

const chunked = <T>(items: ReadonlyArray<T>, size: number): T[][] => {
  const out: T[][] = [];
  for (let i = 0; i < items.length; i += size) out.push(items.slice(i, i + size));
  return out;
};

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

  const atomicOver = (scoped: WorkflowDb): AtomicStorage => {
    const inner = buildOps({ db: scoped, tables, enqueue: enqueueRun });
    return { ...inner.ops, lockRun: inner.lockRun, enqueue: inner.enqueue };
  };

  const idemKey = (name: string, version: number, key: string) =>
    JSON.stringify([name, version, key]);

  const runStartMany = async (scoped: WorkflowDb, specs: ReadonlyArray<StartRunSpec>) => {
    if (specs.length === 0) return [];
    const { runs, events } = tables;

    // Client-generated ids let us correlate the multi-row insert back to input
    // order and tell created rows from idempotent-conflict rows.
    const entries = specs.map((spec) => ({ spec, id: randomUUID() }));

    // 1. Insert the runs, chunked to stay under the bind-param ceiling.
    const createdStatus = new Map<string, RunStatus>();
    for (const group of chunked(entries, RUN_INSERT_CHUNK)) {
      const inserted = await scoped
        .insert(runs)
        .values(
          group.map(({ spec, id }) => ({
            id,
            name: spec.name,
            version: spec.version,
            input: spec.input as object,
            idempotencyKey: spec.idempotencyKey ?? null,
            tags: spec.tags ? [...spec.tags] : null,
            parentRunId: spec.parentRunId ?? null,
            parentCursorKey: spec.parentCursorKey ?? null,
            status: "pending" as const,
          })),
        )
        .onConflictDoNothing({
          target: [runs.name, runs.version, runs.idempotencyKey],
          where: sql`${runs.idempotencyKey} IS NOT NULL`,
        })
        .returning({ id: runs.id, status: runs.status });
      for (const r of inserted) createdStatus.set(r.id, r.status);
    }
    const created = entries.filter((e) => createdStatus.has(e.id));

    // 2. Idempotent conflicts (rows not inserted) resolve to their existing run.
    const conflicts = entries.filter((e) => !createdStatus.has(e.id));
    const existing = new Map<string, { id: string; status: RunStatus }>();
    for (const group of chunked(conflicts, RUN_INSERT_CHUNK)) {
      const rows = await scoped
        .select({
          id: runs.id,
          status: runs.status,
          name: runs.name,
          version: runs.version,
          idempotencyKey: runs.idempotencyKey,
        })
        .from(runs)
        .where(
          or(
            ...group.map((c) =>
              and(
                eq(runs.name, c.spec.name),
                eq(runs.version, c.spec.version),
                eq(runs.idempotencyKey, c.spec.idempotencyKey as string),
              ),
            ),
          ),
        );
      for (const r of rows) {
        if (r.idempotencyKey) {
          existing.set(idemKey(r.name, r.version, r.idempotencyKey), {
            id: r.id,
            status: r.status,
          });
        }
      }
    }

    // 3. Started events + enqueue, for created rows only, same chunking.
    for (const group of chunked(created, RUN_INSERT_CHUNK)) {
      await scoped.insert(events).values(
        group.map(({ spec, id }) => ({
          runId: id,
          type: "started" as const,
          cursorKey: null,
          payload: spec.parentRunId
            ? { parent: spec.parentRunId, parentCursorKey: spec.parentCursorKey }
            : { idempotent: false },
        })),
      );

      const jobs = group.map(({ spec, id }) => ({
        job: { runId: id, name: spec.name, version: spec.version },
        opts: { runAt: spec.runAt, priority: spec.priority },
      }));
      if (enqueue.many) await enqueue.many(scoped, jobs);
      else for (const j of jobs) await enqueue(scoped, j.job, j.opts);
    }

    return entries.map(({ spec, id }) => {
      const status = createdStatus.get(id);
      if (status !== undefined) return { runId: id, status, created: true };
      const hit = existing.get(idemKey(spec.name, spec.version, spec.idempotencyKey as string));
      if (!hit) throw new Error("startManyRuns: conflict but existing row not found");
      return { runId: hit.id, status: hit.status, created: false };
    });
  };

  // Batch is the primitive; `startRun` is a batch of one. `runStartMany` runs in
  // the caller's `tx`, or its own transaction when none is supplied.
  const startMany = (specs: ReadonlyArray<StartRunSpec>, tx?: WorkflowDb) =>
    tx ? runStartMany(tx, specs) : db.transaction((inner) => runStartMany(inner, specs));

  return {
    ...root.ops,

    async transaction(fn) {
      return db.transaction((inner) => fn(atomicOver(inner)));
    },

    async startRun(spec, tx) {
      const [only] = await startMany([spec], tx);
      return only;
    },

    startManyRuns: startMany,

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
