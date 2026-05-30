import type { Pool } from "pg";
import type { FlowDefinition } from "../builder/types";
import {
  createGraphileTxEnqueue,
  type GraphileWorker,
  startGraphileWorker,
  validateCron,
} from "../adapters/graphile";
import type { WorkflowDb } from "../storage/db";
import { createDrizzleStorage, noopEnqueue, type TxEnqueue } from "../storage/drizzle";
import { type Duration, toMs } from "../util/duration";
import { wrapLogger } from "../util/safe-logger";
import { wrapMetrics } from "../util/safe-metrics";
import type { StandardSchemaV1 } from "../util/standard-schema";
import { withTaskSpan } from "../util/tracing";
import type { InternalTables } from "../storage/drizzle";
import { validateLogger, validateRetention, warnIfPoolUndersized } from "./boot-validators";
import { createCancelCascade } from "./cancel-cascade";
import { createHandleFactory } from "./handle";
import { createListenLoop, type ListenLoop } from "./listen-loop";
import { createProgressWaiters, parseProgressPayload } from "./progress-waiters";
import { type RegisteredFlow, FlowRegistry } from "./registry";
import { createRunLifecycle } from "./run-lifecycle";
import { createSchemaVersionCheck } from "./schema-version-check";
import { createSignalRouter } from "./signal-router";
import { createStartChild } from "./start-child";
import { createTerminalWaiters } from "./terminal-waiters";
import type {
  CronSpec,
  DefineFlowOpts,
  FlowContext,
  FlowHandle,
  FlowTables,
  HealthReport,
  ListRunsOpts,
  ListRunsPage,
  Logger,
  MetricsRecorder,
  RunDetail,
  SignalDeliveryResult,
  Storage,
} from "./types";

export type { HealthReport, MetricsRecorder } from "./types";

/** Options for {@link createEngine}. `db`, `pool`, and `tables` are required. */
export interface EngineOpts {
  /** Drizzle handle bound to the Postgres database hosting the workflow tables. */
  db: WorkflowDb;
  /**
   * Postgres connection pool. Caller-owned — `pool.end()` is the caller's
   * responsibility after `engine.stop()`. Size to at least
   * `concurrency + handles awaiting result() + reconciler headroom`.
   */
  pool: Pool;
  /**
   * Optional — pass `flowTables` from `./iterativeflow-schema` only if you
   * customized table names, the `pgSchema` name, or added columns the
   * engine should see. Defaults to the internal `workflow.*` tables.
   */
  tables?: FlowTables;
  /** Structured logger; defaults to a noop. */
  logger?: Logger;
  /** Postgres schema name for graphile-worker. Default `"graphile_worker"`. */
  workerSchema?: string;
  /** Graphile-worker concurrency. Default 5. Should be `<= pool.max`. */
  concurrency?: number;
  /** Graphile-worker poll interval (ms) when no NOTIFY arrives. Default 1000. */
  pollInterval?: number;
  /** Override the transaction-scoped enqueue (test-only — defaults to graphile). */
  enqueue?: TxEnqueue;
  /** Skip auto-scheduling the orphan-reconcile cron. */
  disableReconciler?: boolean;
  /** Reconciler treats runs idle longer than this as candidates for re-enqueue. Default 60_000. */
  reconcilerGraceMs?: number;
  /** Reconciler treats `running` runs untouched longer than this as stuck. Default 600_000. */
  runningStuckMs?: number;
  /**
   * Hard ceiling on a run's `attempts` counter. Once exceeded the run is
   * marked failed with `RUN_ATTEMPTS_EXHAUSTED`. Default 100.
   */
  maxRunAttempts?: number;
  /** Fallback step timeout in ms when `StepOpts.timeoutMs` is not set. */
  defaultStepTimeoutMs?: number;
  /** Auto-schedule pruning crons; opt-in. */
  retention?: {
    /** Drop terminal runs older than this on each sweep. */
    runsOlderThan?: Duration;
    /** Drop event rows older than this on each sweep. */
    eventsOlderThan?: Duration;
    /** Cron schedule for retention sweeps. Default `"0 * * * *"` (hourly). */
    schedule?: string;
    /** Rows deleted per sweep batch. Default 1000. */
    batchSize?: number;
  };
  /** Hard caps; sizes default to none, invoke caps default to 10/1000. */
  limits?: {
    /** Max bytes per `handle.start` input (JSON). */
    maxInputBytes?: number;
    /** Max bytes per `ctx.step` return value (JSON). */
    maxStepResultBytes?: number;
    /** Max bytes per `engine.signal` payload (JSON). */
    maxSignalPayloadBytes?: number;
    /** Maximum `ctx.invoke` chain depth (root = 1). Default 10. */
    maxInvokeDepth?: number;
    /** Maximum direct children spawned by a single run via `ctx.invoke`. Default 1000. */
    maxChildrenPerRun?: number;
  };
  /** Optional telemetry recorder; every method is optional. */
  metrics?: MetricsRecorder;
}

/** Runtime API surface returned by {@link createEngine}. */
export interface Engine {
  /** Register a flow built with `flow(...)` or `defineFlow(...)`. */
  register<I, O>(def: FlowDefinition<I, O> | DefineFlowOpts<I, O>): FlowHandle<I, O>;
  /** Register a cron task. Must be called before {@link Engine.listen}. */
  defineCron(spec: CronSpec): void;
  /** Start consuming the queue. Idempotent. */
  listen(): Promise<void>;
  /** Drain in-flight runs and stop dispatching. Idempotent. */
  stop(): Promise<void>;
  /** Wire `stop()` to the given process signals (default `["SIGTERM","SIGINT"]`). */
  attachShutdownSignals(signals?: ReadonlyArray<NodeJS.Signals>): () => void;
  /**
   * Deliver a payload to an armed signal, or buffer it for a future arm.
   * Returns the {@link SignalDeliveryResult} so callers branch on the outcome.
   */
  signal(runId: string, signalName: string, payload?: unknown): Promise<SignalDeliveryResult>;
  /** Mark a run canceled. Aborts the in-flight step's AbortSignal if running. */
  cancel(runId: string, reason?: string): Promise<void>;
  /** Snapshot of run + steps + timers + signals. */
  status(runId: string): Promise<RunDetail | undefined>;
  /** Liveness ping. */
  health(): Promise<HealthReport>;
  /** Paginated list of runs filtered by name/status/tag/created-window. */
  listRuns(opt?: ListRunsOpts): Promise<ListRunsPage>;
  /** Delete event rows older than `olderThan`. Returns the count removed. */
  pruneEvents(opt: { olderThan: Date; batchSize?: number }): Promise<number>;
  /** Delete terminal-status run rows older than `olderThan`. Returns the count removed. */
  pruneRuns(opt: {
    olderThan: Date;
    status?: ReadonlyArray<"done" | "failed" | "canceled">;
    batchSize?: number;
  }): Promise<number>;
}

const RECONCILE_CRON_NAME = "__iterativeflow_reconcile";
const RETENTION_CRON_NAME = "__iterativeflow_retention";

const noopLogger: Logger = {
  debug: () => undefined,
  info: () => undefined,
  warn: () => undefined,
  error: () => undefined,
};

/** Minimal {@link Logger} that prints structured payloads to `console`. */
export const consoleLogger = (): Logger => ({
  debug: (m, p) => console.debug(m, p ?? {}),
  info: (m, p) => console.info(m, p ?? {}),
  warn: (m, p) => console.warn(m, p ?? {}),
  error: (e, p) => console.error(e, p ?? {}),
});

/** Construct an engine. Wires the registry, storage, worker, and reconciler. */
export const createEngine = (opt: EngineOpts): Engine => {
  const rawLogger = opt.logger ?? noopLogger;
  validateLogger(rawLogger);
  const logger = wrapLogger(rawLogger);
  validateRetention(opt.retention);
  try {
    warnIfPoolUndersized(opt.pool.options?.max, opt.concurrency ?? 5, logger);
  } catch {
    // pg internal shape not available; skip silently
  }

  const registry = new FlowRegistry();
  const enqueue: TxEnqueue = opt.enqueue ?? createGraphileTxEnqueue(opt.workerSchema);
  const tables = opt.tables as unknown as InternalTables | undefined;
  const storage: Storage = createDrizzleStorage({ db: opt.db, logger, enqueue, tables });
  const cronSpecs: CronSpec[] = [];
  const metrics = wrapMetrics(opt.metrics ?? {}, logger);
  const limits = opt.limits ?? {};
  const runControllers = new Map<string, AbortController>();
  const terminalWaiters = createTerminalWaiters();
  const progressWaiters = createProgressWaiters();
  const schemaCheck = createSchemaVersionCheck(storage);
  const startChild = createStartChild(storage);
  const cancelCascade = createCancelCascade({
    storage,
    runControllers,
    notifyTerminal: terminalWaiters.notify,
  });
  const routeSignal = createSignalRouter({
    storage,
    registry,
    logger,
    metrics,
    maxSignalPayloadBytes: limits.maxSignalPayloadBytes,
  });

  const listenLoop: ListenLoop = createListenLoop({
    pool: opt.pool,
    channels: ["flow_terminal", "flow_progress"],
    onNotify: (channel, payload) => {
      if (channel === "flow_terminal") {
        terminalWaiters.notify(payload);
        return;
      }
      if (channel === "flow_progress") {
        const parsed = parseProgressPayload(payload);
        if (parsed) progressWaiters.notify(parsed.runId, parsed.kind, parsed.cursorKey);
      }
    },
    logger,
  });

  const buildHandle = createHandleFactory({
    storage,
    metrics,
    schemaCheck,
    terminalWaiters,
    progressWaiters,
    ensureListen: () => listenLoop.start(),
    maxInputBytes: limits.maxInputBytes,
  });

  const runLifecycle = createRunLifecycle({
    registry,
    storage,
    logger,
    metrics,
    terminalWaiters,
    runControllers,
    maxRunAttempts: opt.maxRunAttempts ?? 100,
    defaultStepTimeoutMs: opt.defaultStepTimeoutMs,
    maxStepResultBytes: limits.maxStepResultBytes,
    maxInvokeDepth: limits.maxInvokeDepth ?? 10,
    maxChildrenPerRun: limits.maxChildrenPerRun ?? 1000,
    startChild,
  });

  let worker: GraphileWorker | null = null;
  let startPromise: Promise<void> | null = null;
  let stopPromise: Promise<void> | null = null;
  let startedAt: Date | undefined;

  const registerDef = <I, O>(
    name: string,
    version: number,
    body: (ctx: FlowContext, input: I) => Promise<O> | O,
    inputSchema: StandardSchemaV1<unknown, I> | undefined,
    nodes?: FlowDefinition<I, O>["nodes"],
    signalSchemas?: FlowDefinition<I, O>["signalSchemas"],
  ): FlowHandle<I, O> => {
    registry.register({
      name,
      version,
      run: body as RegisteredFlow["run"],
      inputSchema: inputSchema as RegisteredFlow["inputSchema"],
      nodes,
      signalSchemas,
    });
    return buildHandle<I, O>(name, version, inputSchema);
  };

  const engine: Engine = {
    register<I, O>(def: FlowDefinition<I, O> | DefineFlowOpts<I, O>): FlowHandle<I, O> {
      if ("body" in def && typeof def.body === "function" && !("nodes" in def)) {
        return registerDef<I, O>(def.name, def.version ?? 1, def.body, def.input);
      }
      const built = def as FlowDefinition<I, O>;
      return registerDef<I, O>(
        built.name,
        built.version,
        built.body,
        built.input,
        built.nodes,
        built.signalSchemas,
      );
    },

    defineCron(spec) {
      if (startPromise !== null) {
        throw new Error(
          `defineCron("${spec.name}") called after engine.listen(); register crons before listen()`,
        );
      }
      if (spec.name === RECONCILE_CRON_NAME || spec.name === RETENTION_CRON_NAME) {
        throw new Error(`cron name "${spec.name}" is reserved by the engine`);
      }
      validateCron(spec.schedule);
      cronSpecs.push(spec);
    },

    async listen() {
      if (startPromise !== null) return startPromise;
      startPromise = (async () => {
        await schemaCheck.ensure();
        const allCrons: CronSpec[] = [...cronSpecs];
        if (!opt.disableReconciler && enqueue !== noopEnqueue) {
          const graceMs = opt.reconcilerGraceMs ?? 60_000;
          const stuckMs = opt.runningStuckMs ?? 10 * 60_000;
          allCrons.push({
            name: RECONCILE_CRON_NAME,
            schedule: "* * * * *",
            run: async () => {
              const reEnqueued = await storage.reenqueueOrphans({
                olderThan: new Date(Date.now() - graceMs),
                runningStuckOlderThan: new Date(Date.now() - stuckMs),
              });
              metrics.reconcilerSweep?.({ scanned: reEnqueued, reEnqueued });
            },
          });
        }
        if (opt.retention) {
          const r = opt.retention;
          const batchSize = r.batchSize ?? 1000;
          allCrons.push({
            name: RETENTION_CRON_NAME,
            schedule: r.schedule ?? "0 * * * *",
            run: async () => {
              if (r.eventsOlderThan !== undefined) {
                await storage.pruneEvents({
                  olderThan: new Date(Date.now() - toMs(r.eventsOlderThan)),
                  batchSize,
                });
              }
              if (r.runsOlderThan !== undefined) {
                await storage.pruneRuns({
                  olderThan: new Date(Date.now() - toMs(r.runsOlderThan)),
                  batchSize,
                });
              }
            },
          });
        }

        worker = await startGraphileWorker({
          pool: opt.pool,
          schema: opt.workerSchema,
          concurrency: opt.concurrency,
          pollInterval: opt.pollInterval,
          logger,
          crons: allCrons,
          runCron: (name, fn) => withTaskSpan("cron", name, fn),
          runWorkflow: (runId) => runLifecycle.execute(runId),
        });
        startedAt = new Date();
        listenLoop.start();
      })();
      try {
        await startPromise;
      } catch (err) {
        startPromise = null;
        throw err;
      }
    },

    async stop() {
      if (stopPromise !== null) return stopPromise;
      stopPromise = (async () => {
        if (worker) {
          await worker.stop();
          worker = null;
        }
        await listenLoop.stop();
        startedAt = undefined;
      })();
      try {
        await stopPromise;
      } finally {
        startPromise = null;
        stopPromise = null;
      }
    },

    attachShutdownSignals(signals = ["SIGTERM", "SIGINT"]) {
      const handler = () => {
        void engine.stop().catch((err) => {
          logger.error(err instanceof Error ? err : new Error(String(err)), {
            event: "flow.shutdown_failed",
          });
        });
      };
      for (const sig of signals) process.on(sig, handler);
      return () => {
        for (const sig of signals) process.off(sig, handler);
      };
    },

    signal: (runId, signalName, payload) => routeSignal(runId, signalName, payload),

    status: (runId) => storage.loadRunDetail(runId),

    listRuns: (o = {}) => storage.listRuns(o),

    cancel: (runId, reason) => cancelCascade(runId, reason),

    async health() {
      let db = false;
      try {
        await opt.pool.query("SELECT 1");
        db = true;
      } catch (err) {
        logger.warn("flow.health.db_failed", {
          message: err instanceof Error ? err.message : String(err),
        });
      }
      const workerOk = worker !== null;
      const listen = listenLoop.state() === "listening";
      return { ok: db && workerOk, db, worker: workerOk, listen, startedAt };
    },

    pruneEvents: (o) => storage.pruneEvents(o),
    pruneRuns: (o) => storage.pruneRuns(o),
  };

  return engine;
};

/** Hand-written flow factory — same as `flow().build()` for the simple `(ctx, input) => out` case. */
export const defineFlow = <I, O>(opts: DefineFlowOpts<I, O>): DefineFlowOpts<I, O> => opts;
