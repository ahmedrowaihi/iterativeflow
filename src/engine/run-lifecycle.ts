import { FlowRuntimeError, toFlowError } from "../util/errors";
import { withTaskSpan } from "../util/tracing";
import { formatIssues, validate } from "../util/standard-schema";
import { RuntimeFlowContext } from "./context";
import { checkCompat } from "./cursor";
import type { FlowRegistry } from "./registry";
import { isSuspend, type FlowSuspend } from "./suspend";
import type { FlowError } from "../storage/schema";
import type { TerminalWaiters } from "./terminal-waiters";
import type { FlowHandle, InvokeOpts, Logger, MetricsRecorder, Storage } from "./types";

/** @internal */
export interface RunAttemptResult {
  status: "completed" | "suspended" | "failed" | "skipped";
  wakeAt?: Date;
  wakeOn?: string;
}

/** @internal */
export interface RunLifecycleDeps {
  registry: FlowRegistry;
  storage: Storage;
  logger: Logger;
  metrics?: MetricsRecorder;
  terminalWaiters?: TerminalWaiters;
  runControllers?: Map<string, AbortController>;
  abortSignal?: AbortSignal;
  maxRunAttempts: number;
  defaultStepTimeoutMs?: number;
  maxStepResultBytes?: number;
  maxInvokeDepth: number;
  maxChildrenPerRun: number;
  startChild?: (
    parentRunId: string,
    parentCursorKey: string,
    childHandle: FlowHandle<unknown, unknown>,
    input: unknown,
    opts: InvokeOpts | undefined,
  ) => Promise<string>;
}

const notify = (deps: RunLifecycleDeps, runId: string): void => {
  deps.terminalWaiters?.notify(runId);
};

const recordMetric = (deps: RunLifecycleDeps, fn: keyof MetricsRecorder, payload: object): void => {
  const m = deps.metrics;
  if (!m) return;
  const method = m[fn] as ((p: object) => void) | undefined;
  method?.(payload);
};

const endRun = async (
  deps: RunLifecycleDeps,
  runId: string,
  flow: { name: string; version: number },
  status: "completed" | "failed",
  payload: { output?: unknown; error?: FlowError; durationMs?: number },
): Promise<void> => {
  try {
    if (status === "completed") {
      await deps.storage.markCompleted(runId, payload.output);
      await deps.storage.recordEvent({ runId, type: "completed", payload: null });
      recordMetric(deps, "runCompleted", {
        name: flow.name,
        version: flow.version,
        durationMs: payload.durationMs ?? 0,
      });
    } else {
      await deps.storage.markFailed(runId, payload.error!);
      await deps.storage.recordEvent({ runId, type: "failed", payload: null });
      recordMetric(deps, "runFailed", {
        name: flow.name,
        version: flow.version,
        code: payload.error!.code,
      });
    }
    await deps.storage.notifyTerminal(runId);
  } catch (markErr) {
    deps.logger.error(markErr instanceof Error ? markErr : new Error(String(markErr)), {
      runId,
      event: `flow.${status === "completed" ? "markCompleted" : "markFailed"}_failed`,
    });
  } finally {
    notify(deps, runId);
  }
};

const handleSuspend = async (
  deps: RunLifecycleDeps,
  runId: string,
  err: FlowSuspend,
): Promise<RunAttemptResult> => {
  await deps.storage.transaction(async (tx) => {
    await tx.lockRun(runId);
    if (err.reason === "awaiting_signal") {
      await tx.markAwaitingSignal(runId);
    } else if (err.reason === "step_retry") {
      await tx.markRetrying(runId);
    } else {
      await tx.markSleeping(runId);
    }
    await tx.recordEvent({
      runId,
      type: "suspended",
      payload: { reason: err.reason, wakeAt: err.wakeAt, wakeOn: err.wakeOn },
    });
    if (err.wakeAt) {
      await tx.enqueue(runId, { runAt: err.wakeAt });
    }
  });
  return { status: "suspended", wakeAt: err.wakeAt, wakeOn: err.wakeOn };
};

/**
 * Execute one attempt of `runId`: claim, play the body, finalize. Returns the
 * outcome so callers (and tests) can branch. The production orchestrator
 * {@link createRunLifecycle} wraps this with a tracing span.
 *
 * @internal
 */
export const playRunAttempt = async (
  deps: RunLifecycleDeps,
  runId: string,
): Promise<RunAttemptResult> => {
  const claim = await deps.storage.claimRun(runId);
  if (claim.kind === "missing") {
    deps.logger.warn("flow.run_not_found", { runId });
    return { status: "skipped" };
  }
  if (claim.kind === "terminal") return { status: "skipped" };
  if (claim.kind === "lost") {
    deps.logger.warn("flow.run_claim_lost", { runId });
    return { status: "skipped" };
  }

  const { run, snapshot, resumed } = claim.claim;
  const flow = { name: run.name, version: run.version };

  if (run.attempts > deps.maxRunAttempts) {
    await endRun(deps, runId, flow, "failed", {
      error: {
        code: "RUN_ATTEMPTS_EXHAUSTED",
        message: `Run "${run.name}" exceeded maxRunAttempts=${deps.maxRunAttempts}`,
      },
    });
    return { status: "failed" };
  }

  const def = deps.registry.get(run.name, run.version);
  if (!def) {
    await endRun(deps, runId, flow, "failed", {
      error: {
        code: "FLOW_UNKNOWN",
        message: `No flow registered for "${run.name}@${run.version}"`,
      },
    });
    return { status: "failed" };
  }

  let input = run.input;
  if (def.inputSchema) {
    const parsed = await validate(def.inputSchema, run.input);
    if (parsed.issues) {
      await endRun(deps, runId, flow, "failed", {
        error: {
          code: "INPUT_INVALID",
          message: `Flow input failed schema: ${formatIssues(parsed.issues)}`,
        },
      });
      return { status: "failed" };
    }
    input = parsed.value;
  }

  if (def.nodes) {
    const incompat = checkCompat(snapshot, def.nodes);
    if (incompat) {
      await endRun(deps, runId, flow, "failed", { error: incompat });
      return { status: "failed" };
    }
  }

  await deps.storage.recordEvent({ runId, type: resumed ? "resumed" : "started" });

  const startMs = Date.now();
  const controller = new AbortController();
  if (deps.runControllers) deps.runControllers.set(runId, controller);
  const abortSignal = deps.runControllers
    ? controller.signal
    : (deps.abortSignal ?? controller.signal);

  try {
    const ctx = new RuntimeFlowContext({
      runId,
      attempt: run.attempts,
      storage: deps.storage,
      snapshot,
      logger: deps.logger,
      abortSignal,
      defaultStepTimeoutMs: deps.defaultStepTimeoutMs,
      maxStepResultBytes: deps.maxStepResultBytes,
      maxInvokeDepth: deps.maxInvokeDepth,
      maxChildrenPerRun: deps.maxChildrenPerRun,
      metrics: deps.metrics,
      flow,
      startChild: deps.startChild
        ? (cursorKey, childHandle, childInput, opts) =>
            deps.startChild!(runId, cursorKey, childHandle, childInput, opts)
        : undefined,
    });

    const output = await def.run(ctx, input);
    await endRun(deps, runId, flow, "completed", { output, durationMs: Date.now() - startMs });
    return { status: "completed" };
  } catch (err) {
    if (isSuspend(err)) {
      recordMetric(deps, "runSuspended", {
        name: flow.name,
        version: flow.version,
        reason: err.reason,
      });
      return handleSuspend(deps, runId, err);
    }
    const error = toFlowError(err);
    if (!(err instanceof FlowRuntimeError)) {
      deps.logger.error(err instanceof Error ? err : new Error(String(err)), {
        runId,
        event: "flow.unhandled_error",
      });
    }
    await endRun(deps, runId, flow, "failed", { error });
    return { status: "failed" };
  } finally {
    if (deps.runControllers) deps.runControllers.delete(runId);
  }
};

/** @internal */
export interface RunLifecycle {
  execute(runId: string): Promise<void>;
}

/**
 * Production orchestrator: wraps {@link playRunAttempt} in an OpenTelemetry
 * span and discards the outcome (graphile-worker doesn't consume it).
 *
 * @internal
 */
export const createRunLifecycle = (deps: RunLifecycleDeps): RunLifecycle => ({
  execute: (runId) =>
    withTaskSpan("flow", runId, async () => {
      await playRunAttempt(deps, runId);
    }),
});
