import type { Pool } from "pg";
import type { FlowDefinition } from "../builder/types";
import { withTaskSpan } from "../tracing";
import {
  createGraphileTxEnqueue,
  type GraphileWorker,
  startGraphileWorker,
} from "../runtime/graphile";
import type { WorkflowDb } from "../storage/db";
import { createDrizzleStorage, noopEnqueue, type TxEnqueue } from "../storage/drizzle";
import { toFireAt } from "../util/duration";
import { formatIssues, type StandardSchemaV1, validate } from "../util/standard-schema";
import { type RegisteredWorkflow, WorkflowRegistry } from "./registry";
import { executeRun } from "./runner";
import type {
  CronSpec,
  DefineWorkflowOpts,
  Logger,
  RunDetail,
  StartOpts,
  Storage,
  WorkflowHandle,
} from "./types";

export type { TaskKind } from "../tracing";

export interface EngineOpts {
  db: WorkflowDb;
  pool: Pool;
  logger: Logger;
  workerSchema?: string;
  concurrency?: number;
  pollInterval?: number;
  enqueue?: TxEnqueue;
  disableReconciler?: boolean;
  reconcilerGraceMs?: number;
  runningStuckMs?: number;
}

export interface Engine {
  register<I, O>(def: FlowDefinition<I, O>): WorkflowHandle<I, O>;
  defineWorkflow<I, O>(opts: DefineWorkflowOpts<I, O>): WorkflowHandle<I, O>;
  defineCron(spec: CronSpec): void;
  start(): Promise<void>;
  stop(): Promise<void>;
  signal(runId: string, hookName: string, payload?: unknown): Promise<void>;
  cancel(runId: string, reason?: string): Promise<void>;
  status(runId: string): Promise<RunDetail | undefined>;
  pruneEvents(opt: { olderThan: Date; batchSize?: number }): Promise<number>;
  pruneRuns(opt: {
    olderThan: Date;
    status?: ReadonlyArray<"done" | "failed" | "canceled">;
    batchSize?: number;
  }): Promise<number>;
}

const RECONCILE_CRON_NAME = "__iterativeflow_reconcile";

export const createEngine = (opt: EngineOpts): Engine => {
  const registry = new WorkflowRegistry();
  const enqueue: TxEnqueue = opt.enqueue ?? createGraphileTxEnqueue(opt.workerSchema);
  const storage: Storage = createDrizzleStorage({
    db: opt.db,
    logger: opt.logger,
    enqueue,
  });
  const cronSpecs: CronSpec[] = [];
  let worker: GraphileWorker | null = null;
  let started = false;

  const buildHandle = <I, O>(
    name: string,
    version: number,
    inputSchema: StandardSchemaV1<unknown, I> | undefined,
  ): WorkflowHandle<I, O> => ({
    name,
    version,
    async start(input, startOpts: StartOpts = {}) {
      let validated: I = input;
      if (inputSchema) {
        const parsed = await validate(inputSchema, input);
        if (parsed.issues) {
          throw new Error(`Workflow "${name}" input failed schema: ${formatIssues(parsed.issues)}`);
        }
        validated = parsed.value;
      }
      const runAt = startOpts.delay ? toFireAt(startOpts.delay) : undefined;

      return storage.transaction(async (tx) => {
        const { runId, status, created } = await tx.createRun({
          name,
          version,
          input: validated,
          idempotencyKey: startOpts.idempotencyKey,
        });
        if (created) {
          await tx.recordEvent({
            runId,
            type: "started",
            payload: { idempotent: false },
          });
          await tx.enqueue(runId, { runAt, priority: startOpts.priority });
        }
        return { runId, status };
      });
    },
    output: (runId) => storage.loadOutput(runId) as Promise<O | undefined>,
  });

  return {
    register<I, O>(def: FlowDefinition<I, O>): WorkflowHandle<I, O> {
      registry.register({
        name: def.name,
        version: def.version,
        run: def.run as RegisteredWorkflow["run"],
        inputSchema: def.input as RegisteredWorkflow["inputSchema"],
        nodes: def.nodes,
      });
      return buildHandle<I, O>(def.name, def.version, def.input);
    },

    defineWorkflow<I, O>(opts: DefineWorkflowOpts<I, O>): WorkflowHandle<I, O> {
      const version = opts.version ?? 1;
      registry.register({
        name: opts.name,
        version,
        run: opts.run as RegisteredWorkflow["run"],
        inputSchema: opts.input as RegisteredWorkflow["inputSchema"],
      });
      return buildHandle<I, O>(opts.name, version, opts.input);
    },

    defineCron(spec) {
      if (started) {
        throw new Error(
          `defineCron("${spec.name}") called after engine.start(); register crons before start()`,
        );
      }
      if (spec.name === RECONCILE_CRON_NAME) {
        throw new Error(`cron name "${RECONCILE_CRON_NAME}" is reserved by the engine reconciler`);
      }
      cronSpecs.push(spec);
    },

    async start() {
      if (started) return;

      const allCrons: CronSpec[] = [...cronSpecs];
      if (!opt.disableReconciler && enqueue !== noopEnqueue) {
        const graceMs = opt.reconcilerGraceMs ?? 60_000;
        const stuckMs = opt.runningStuckMs ?? 10 * 60_000;
        allCrons.push({
          name: RECONCILE_CRON_NAME,
          schedule: "* * * * *",
          run: () =>
            storage
              .reenqueueOrphans({
                olderThan: new Date(Date.now() - graceMs),
                runningStuckOlderThan: new Date(Date.now() - stuckMs),
              })
              .then(() => undefined),
        });
      }

      worker = await startGraphileWorker({
        pool: opt.pool,
        schema: opt.workerSchema,
        concurrency: opt.concurrency,
        pollInterval: opt.pollInterval,
        logger: opt.logger,
        crons: allCrons,
        runCron: (name, fn) => withTaskSpan("cron", name, fn),
        runWorkflow: (runId) =>
          withTaskSpan("workflow", runId, () =>
            executeRun({ registry, storage, logger: opt.logger }, runId).then(() => undefined),
          ),
      });
      started = true;
    },

    async stop() {
      if (worker) {
        await worker.stop();
        worker = null;
      }
      started = false;
    },

    async signal(runId, hookName, payload) {
      await storage.signalHook(runId, hookName, payload);
    },

    status: (runId) => storage.loadRunDetail(runId),

    async cancel(runId, reason) {
      await storage.transaction(async (tx) => {
        await tx.lockRun(runId);
        await tx.markCanceled(runId, reason);
        await tx.recordEvent({
          runId,
          type: "canceled",
          payload: { reason },
        });
      });
    },

    pruneEvents: (o) => storage.pruneEvents(o),
    pruneRuns: (o) => storage.pruneRuns(o),
  };
};
