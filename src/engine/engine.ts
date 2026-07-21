import type { Pool } from "pg";
import type { FlowContract } from "../builder/contract";
import type { FlowDefinition } from "../builder/types";
import {
  createGraphileDispatcher,
  createGraphileTxEnqueue,
  validateCron,
} from "../adapters/graphile";
import type { Dispatcher } from "./scheduler";
import type { WorkflowDb } from "../storage/db";
import { createDrizzleStorage, noopEnqueue, type TxEnqueue } from "../storage/drizzle";
import { type Duration, toMs } from "../util/duration";
import { wrapLogger } from "../util/safe-logger";
import { wrapMetrics } from "../util/safe-metrics";
import type { StandardSchemaV1 } from "../util/standard-schema";
import { withTaskSpan } from "../util/tracing";
import type { InternalTables } from "../storage/drizzle";
import {
  validateLogger,
  validateRetention,
  warnIfNoRetention,
  warnIfPoolUndersized,
  warnIfStuckShorterThanStepTimeout,
  warnIfUnboundedStepTimeout,
} from "./boot-validators";
import { createCancelCascade } from "./cancel-cascade";
import { createHandleFactory } from "./handle";
import {
  buildReconcilerCron,
  buildRetentionCron,
  RECONCILE_CRON_NAME,
  RETENTION_CRON_NAME,
} from "./internal-crons";
import { createListenLoop, type ListenLoop } from "./listen-loop";
import { fallbackLogger } from "./loggers";
import { createProgressWaiters, parseProgressPayload } from "./progress-waiters";
import { type RegisteredFlow, FlowRegistry } from "./registry";
import { createRunLifecycle } from "./run-lifecycle";
import { createSchemaVersionCheck } from "./schema-version-check";
import { createSignalRouter } from "./signal-router";
import { createStartChild } from "./start-child";
import { createTerminalWaiters } from "./terminal-waiters";
import type {
  CronSpec,
  DefaultFlowTables,
  DefineFlowOpts,
  FlowContext,
  FlowHandle,
  FlowTables,
  HealthReport,
  ListRunsOpts,
  ListRunsPage,
  Logger,
  MetricsRecorder,
  RetryResult,
  RunDetail,
  SignalDeliveryResult,
  StartOpts,
  Storage,
} from "./types";
import type { RunStatus } from "../storage/schema";

export type { HealthReport, MetricsRecorder } from "./types";

/**
 * Options for {@link createEngine}. `db` and `pool` are required; `tables`
 * is optional and defaults to the internal `workflow.*` table objects.
 *
 * Pass `tables` to make `engine.status()` and `engine.listRuns()` return
 * rows with your drizzle-inferred types (including custom columns).
 */
export interface EngineOpts<T extends FlowTables = DefaultFlowTables> {
  /** Drizzle handle bound to the Postgres database hosting the workflow tables. */
  db: WorkflowDb;
  /**
   * Postgres connection pool. Caller-owned — `pool.end()` is the caller's
   * responsibility after `engine.stop()`. Size to at least
   * `worker.concurrency + handles awaiting result() + reconciler headroom`.
   */
  pool: Pool;
  /**
   * Optional — pass `flowTables` from `./iterativeflow-schema` only if you
   * customized table names, the `pgSchema` name, or added columns the
   * engine should see. Defaults to the internal `workflow.*` tables.
   */
  tables?: T;
  /** Structured logger; defaults to a noop. */
  logger?: Logger;
  /** Optional telemetry recorder; every method is optional. */
  metrics?: MetricsRecorder;

  /** graphile-worker runner settings. Omit for defaults. */
  worker?: {
    /** Postgres schema for graphile-worker. Default `"graphile_worker"`. */
    schema?: string;
    /** Worker concurrency. Default 5. Should be `<= pool.max`. */
    concurrency?: number;
    /** Poll interval (ms) when no NOTIFY arrives. Default 1000. */
    pollInterval?: number;
    /** Override the transaction-scoped enqueue (advanced/test — defaults to graphile). */
    enqueue?: TxEnqueue;
  };

  /**
   * Override the run {@link Dispatcher} (advanced — defaults to the resident
   * graphile worker). Swap for a serverless dispatcher that exposes
   * `engine.handleRun` to an HTTP route instead of polling.
   */
  dispatcher?: Dispatcher;

  /**
   * How `handle.result()` / `handle.wait()` deliver completion.
   *
   * - `"listen"` (default) — block on the `flow_terminal` LISTEN channel; needs
   *   a resident process.
   * - `"poll"` — there is no resident listener (serverless): `result()`/`wait()`
   *   on a non-terminal run throw, directing callers to `engine.status(runId)`
   *   or a terminal webhook. `result()` on an already-terminal run still returns.
   */
  results?: "listen" | "poll";

  /**
   * Orphan reconciler — re-enqueues runs whose status looks stuck. **ON by
   * default.** Pass `false` to disable, or an object to tune.
   */
  reconciler?:
    | false
    | {
        /** Sweep cadence (5-field cron). Default `"* * * * *"` (every minute). */
        schedule?: string;
        /** Runs idle longer than this are re-enqueue candidates. Default 60_000. */
        graceMs?: number;
        /** `running` runs untouched longer than this are treated as stuck. Default 600_000. */
        runningStuckMs?: number;
      };

  /**
   * Retention sweeps — **delete** old terminal runs and event rows so the
   * tables don't grow unbounded. **OFF by default.** Pass an object to enable;
   * `false` is the same as omitting it.
   */
  retention?:
    | false
    | {
        /** Drop terminal runs older than this on each sweep. */
        runsOlderThan?: Duration;
        /** Drop event rows older than this on each sweep. */
        eventsOlderThan?: Duration;
        /** Sweep cadence (5-field cron). Default `"0 * * * *"` (hourly). */
        schedule?: string;
        /** Rows deleted per sweep batch. Default 1000. */
        batchSize?: number;
      };

  /** Hard caps on execution and payload sizes. Omit for defaults. */
  limits?: {
    /**
     * Ceiling on a run's `attempts`; exceeding it fails the run with
     * `RUN_ATTEMPTS_EXHAUSTED`. Default 100.
     */
    maxRunAttempts?: number;
    /** Fallback step timeout (ms) when `StepOpts.timeoutMs` is unset. */
    defaultStepTimeoutMs?: number;
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
}

/**
 * Runtime API surface returned by {@link createEngine}. Generic over the
 * caller's {@link FlowTables} so `status()` and `listRuns()` return rows
 * with the consumer's drizzle-inferred row types.
 */
export interface Engine<T extends FlowTables = DefaultFlowTables> {
  /** Register a flow built with `flow(...)` or `defineFlow(...)`. */
  register<I, O>(def: FlowDefinition<I, O> | DefineFlowOpts<I, O>): FlowHandle<I, O>;
  /**
   * Build a typed enqueue-only {@link FlowHandle} from a {@link FlowContract}
   * without registering a body. The process can `.start`/`.result` the flow but
   * never claims it (it adds nothing to the worker's task list) — the worker
   * that {@link Engine.register}ed the body runs it. Lets an API process start
   * flows from the light contract without importing the body's heavy deps.
   */
  enqueueHandle<I, O>(contract: FlowContract<I, O>): FlowHandle<I, O>;
  /**
   * Low-level, untyped escape hatch behind {@link Engine.enqueueHandle}, for
   * dynamic/codegen callers with no static contract. Prefer `enqueueHandle`.
   */
  enqueue(
    name: string,
    version: number,
    input: unknown,
    opts?: StartOpts,
  ): Promise<{ runId: string; status: RunStatus }>;
  /** Register a cron task. Must be called before {@link Engine.listen}. */
  defineCron(spec: CronSpec): void;
  /**
   * Run one stateless claim → replay → run-to-suspend → persist cycle for a
   * run. The resident dispatcher calls this from its poll loop; a serverless
   * dispatcher exposes it to an HTTP route so one invocation advances the run,
   * then exits.
   */
  handleRun(runId: string): Promise<void>;
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
  /**
   * Re-enqueue a `failed` run for replay. Memoized `ok` step results are
   * preserved, the `failed_terminal` step row is deleted, the run is reset
   * to `pending` with `attempts=0`. The body re-executes from the failing
   * step. Returns the {@link RetryResult} so callers can branch on whether
   * the run was actually queued, missing, or not in `failed` status.
   */
  retry(runId: string): Promise<RetryResult>;
  /**
   * Re-enqueue runs whose status looks stuck (orphaned by a crash between a
   * drain and `handleRun`, or a worker that died mid-run). The resident
   * dispatcher runs this on a cron; a serverless host drives it from a scheduled
   * `/cron` trigger. Returns how many runs were re-enqueued.
   */
  reconcile(): Promise<{ reEnqueued: number }>;
  /** Snapshot of run + steps + timers + signals. */
  status(runId: string): Promise<RunDetail<T> | undefined>;
  /** Liveness ping. */
  health(): Promise<HealthReport>;
  /** Paginated list of runs filtered by name/status/tag/created-window. */
  listRuns(opt?: ListRunsOpts): Promise<ListRunsPage<T>>;
  /** Delete event rows older than `olderThan`. Returns the count removed. */
  pruneEvents(opt: { olderThan: Date; batchSize?: number }): Promise<number>;
  /** Delete terminal-status run rows older than `olderThan`. Returns the count removed. */
  pruneRuns(opt: {
    olderThan: Date;
    status?: ReadonlyArray<"done" | "failed" | "canceled">;
    batchSize?: number;
  }): Promise<number>;
}

// Defaults live here so boot validators and the listen() body agree.
const DEFAULT_CONCURRENCY = 5;
const DEFAULT_RECONCILER_GRACE_MS = toMs("1m");
const DEFAULT_RUNNING_STUCK_MS = toMs("10m");
const DEFAULT_MAX_RUN_ATTEMPTS = 100;
const DEFAULT_MAX_INVOKE_DEPTH = 10;
const DEFAULT_MAX_CHILDREN_PER_RUN = 1000;
const DEFAULT_RECONCILER_SCHEDULE = "* * * * *";

interface ReconcilerConfig {
  enabled: boolean;
  schedule: string;
  graceMs: number;
  stuckMs: number;
}

/** Fill the `false | {...}` reconciler option with its defaults. */
const withReconcilerDefaults = (opt: EngineOpts["reconciler"]): ReconcilerConfig => {
  const tuning = typeof opt === "object" ? opt : undefined;
  return {
    enabled: opt !== false,
    schedule: tuning?.schedule ?? DEFAULT_RECONCILER_SCHEDULE,
    graceMs: tuning?.graceMs ?? DEFAULT_RECONCILER_GRACE_MS,
    stuckMs: tuning?.runningStuckMs ?? DEFAULT_RUNNING_STUCK_MS,
  };
};

export { consoleLogger } from "./loggers";

/**
 * Construct an engine. Wires the registry, storage, worker, and reconciler.
 *
 * Generic over `T extends FlowTables`: when you pass `tables`, the row
 * types returned by `engine.status()` and `engine.listRuns()` reflect
 * your drizzle schema (including columns you added). Without `tables`,
 * `T` defaults to the engine's internal table objects.
 */
export const createEngine = <T extends FlowTables = DefaultFlowTables>(
  opt: EngineOpts<T>,
): Engine<T> => {
  const rawLogger = opt.logger ?? fallbackLogger;
  validateLogger(rawLogger);
  const logger = wrapLogger(rawLogger);

  const workerCfg = opt.worker ?? {};
  const limits = opt.limits ?? {};
  const maxRunAttempts = limits.maxRunAttempts ?? DEFAULT_MAX_RUN_ATTEMPTS;
  const reconciler = withReconcilerDefaults(opt.reconciler);
  const retention = opt.retention === false ? undefined : opt.retention;

  validateRetention(retention);
  try {
    warnIfPoolUndersized(
      opt.pool.options?.max,
      workerCfg.concurrency ?? DEFAULT_CONCURRENCY,
      logger,
    );
  } catch {
    // pg internal shape not available; skip silently
  }
  warnIfStuckShorterThanStepTimeout(reconciler.stuckMs, limits.defaultStepTimeoutMs, logger);
  warnIfUnboundedStepTimeout(limits.defaultStepTimeoutMs, logger);
  warnIfNoRetention(retention, logger);

  const registry = new FlowRegistry();
  const enqueue: TxEnqueue = workerCfg.enqueue ?? createGraphileTxEnqueue(workerCfg.schema);
  const tables = opt.tables as unknown as InternalTables | undefined;
  const storage: Storage = createDrizzleStorage({ db: opt.db, logger, enqueue, tables });
  const cronSpecs: CronSpec[] = [];
  const metrics = wrapMetrics(opt.metrics ?? {}, logger);
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

  const pollResults = opt.results === "poll";
  const buildHandle = createHandleFactory({
    storage,
    metrics,
    schemaCheck,
    terminalWaiters,
    progressWaiters,
    ensureListen: pollResults
      ? () => {
          throw new Error(
            "result()/wait() need the resident LISTEN dispatcher; with results: 'poll', poll engine.status(runId) or use a terminal webhook instead",
          );
        }
      : () => listenLoop.start(),
    maxInputBytes: limits.maxInputBytes,
  });

  const runLifecycle = createRunLifecycle({
    registry,
    storage,
    logger,
    metrics,
    terminalWaiters,
    runControllers,
    maxRunAttempts,
    defaultStepTimeoutMs: limits.defaultStepTimeoutMs,
    maxStepResultBytes: limits.maxStepResultBytes,
    maxInvokeDepth: limits.maxInvokeDepth ?? DEFAULT_MAX_INVOKE_DEPTH,
    maxChildrenPerRun: limits.maxChildrenPerRun ?? DEFAULT_MAX_CHILDREN_PER_RUN,
    startChild,
  });

  const dispatcher: Dispatcher =
    opt.dispatcher ??
    createGraphileDispatcher({
      pool: opt.pool,
      db: opt.db,
      logger,
      schema: workerCfg.schema,
      concurrency: workerCfg.concurrency,
      pollInterval: workerCfg.pollInterval,
    });
  const handleRun = (runId: string): Promise<void> => runLifecycle.execute(runId);

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
    if (startPromise !== null) {
      throw new Error(
        `register("${name}") called after engine.listen(); the worker's task list is fixed at listen() — register all flows before listen()`,
      );
    }
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

  const engine: Engine<T> = {
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

    enqueueHandle<I, O>(contract: FlowContract<I, O>): FlowHandle<I, O> {
      return buildHandle<I, O>(contract.name, contract.version, contract.input);
    },

    enqueue(name, version, input, opts) {
      return buildHandle<unknown, unknown>(name, version, undefined).start(input, opts);
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

    handleRun,

    async listen() {
      if (startPromise !== null) return startPromise;
      startPromise = (async () => {
        await schemaCheck.ensure();
        const allCrons: CronSpec[] = [...cronSpecs];
        if (reconciler.enabled && enqueue !== noopEnqueue) {
          allCrons.push(
            buildReconcilerCron({
              storage,
              metrics,
              schedule: reconciler.schedule,
              graceMs: reconciler.graceMs,
              stuckMs: reconciler.stuckMs,
              maxRunAttempts,
            }),
          );
        }
        if (retention) {
          allCrons.push(buildRetentionCron({ storage, retention }));
        }

        await dispatcher.start({
          handleRun,
          crons: allCrons,
          flows: registry.list(),
          runCron: (name, fn) => withTaskSpan("cron", name, fn),
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
        await dispatcher.stop();
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

    status: (runId) => storage.loadRunDetail(runId) as Promise<RunDetail<T> | undefined>,

    listRuns: (o = {}) => storage.listRuns(o) as Promise<ListRunsPage<T>>,

    cancel: (runId, reason) => cancelCascade(runId, reason),

    retry: (runId) => storage.retryRun(runId),

    async reconcile() {
      const reEnqueued = await storage.reenqueueOrphans({
        olderThan: new Date(Date.now() - reconciler.graceMs),
        runningStuckOlderThan: new Date(Date.now() - reconciler.stuckMs),
        maxRunAttempts,
      });
      metrics.reconcilerSweep?.({ scanned: reEnqueued, reEnqueued });
      return { reEnqueued };
    },

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
      const workerOk = dispatcher.running();
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
