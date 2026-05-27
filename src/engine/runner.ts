import type { FlowNode } from "../builder/types";
import { toWorkflowError, WorkflowRuntimeError } from "../util/errors";
import { formatIssues, validate } from "../util/standard-schema";
import { RuntimeWorkflowContext } from "./context";
import type { WorkflowRegistry } from "./registry";
import type { WorkflowSuspend } from "./suspend";
import { isSuspend } from "./suspend";
import type { Logger, RunSnapshot, Storage } from "./types";
import type { WorkflowError } from "../storage/schema";

export interface RunResult {
  status: "completed" | "suspended" | "failed" | "skipped";
  wakeAt?: Date;
  wakeOn?: string;
}

interface RunnerDeps {
  registry: WorkflowRegistry;
  storage: Storage;
  logger: Logger;
}

const hasLoop = (nodes: ReadonlyArray<FlowNode>): boolean => nodes.some((n) => n.kind === "loop");

const producibleKeys = (nodes: ReadonlyArray<FlowNode>): Set<string> => {
  const cursors = new Map<string, number>();
  const next = (base: string): string => {
    const idx = cursors.get(base) ?? 0;
    cursors.set(base, idx + 1);
    return idx === 0 ? base : `${base}:${idx}`;
  };

  const keys = new Set<string>();
  for (const node of nodes) {
    if (node.kind === "step") keys.add(next(node.name));
    else if (node.kind === "sleep") keys.add(next("sleep"));
    else if (node.kind === "hook") keys.add(next(`hook:${node.name}`));
  }
  return keys;
};

const baseOf = (key: string): string => {
  const i = key.lastIndexOf(":");
  if (i === -1) return key;
  return /^\d+$/.test(key.slice(i + 1)) ? key.slice(0, i) : key;
};

const checkCompat = (
  snapshot: RunSnapshot,
  nodes: ReadonlyArray<FlowNode>,
): WorkflowError | null => {
  const producible = producibleKeys(nodes);
  const bases = new Set<string>();
  for (const key of producible) bases.add(baseOf(key));

  const recorded = [...snapshot.steps.keys(), ...snapshot.timers.keys(), ...snapshot.hooks.keys()];

  for (const key of recorded) {
    if (producible.has(key)) continue;
    if (bases.has(baseOf(key))) {
      return {
        code: "NON_DETERMINISTIC",
        message: `replay diverged: recorded step "${key}" is not produced by the current graph — occurrence count for "${baseOf(key)}" changed`,
      };
    }
    return {
      code: "INCOMPATIBLE_VERSION",
      message: `recorded step "${key}" is absent from the registered flow graph — it was removed or renamed`,
    };
  }
  return null;
};

export const executeRun = async (deps: RunnerDeps, runId: string): Promise<RunResult> => {
  const run = await deps.storage.loadRun(runId);
  if (!run) {
    deps.logger.warn("workflow.run_not_found", { runId });
    return { status: "skipped" };
  }
  if (run.status === "done" || run.status === "failed" || run.status === "canceled") {
    return { status: "skipped" };
  }

  const def = deps.registry.get(run.name, run.version);
  if (!def) {
    const error = {
      code: "UNKNOWN_WORKFLOW",
      message: `No workflow registered for "${run.name}@${run.version}"`,
    };
    await deps.storage.markFailed(runId, error);
    await deps.storage.recordEvent({ runId, type: "failed", payload: error });
    return { status: "failed" };
  }

  let input = run.input;
  if (def.inputSchema) {
    const parsed = await validate(def.inputSchema, run.input);
    if (parsed.issues) {
      const error = {
        code: "INPUT_INVALID",
        message: `Workflow input failed schema: ${formatIssues(parsed.issues)}`,
      };
      await deps.storage.markFailed(runId, error);
      await deps.storage.recordEvent({ runId, type: "failed", payload: error });
      return { status: "failed" };
    }
    input = parsed.value;
  }

  await deps.storage.markRunning(runId);
  if (run.status === "pending") {
    await deps.storage.recordEvent({ runId, type: "started" });
  } else {
    await deps.storage.recordEvent({ runId, type: "resumed" });
  }

  try {
    const snapshot = await deps.storage.loadSnapshot(runId);

    if (def.nodes && !hasLoop(def.nodes)) {
      const incompat = checkCompat(snapshot, def.nodes);
      if (incompat) {
        await deps.storage.markFailed(runId, incompat);
        await deps.storage.recordEvent({ runId, type: "failed", payload: null });
        return { status: "failed" };
      }
    }

    const ctx = new RuntimeWorkflowContext({
      runId,
      attempt: run.attempts + 1,
      storage: deps.storage,
      snapshot,
      logger: deps.logger,
    });

    const output = await def.run(ctx, input);
    await deps.storage.markCompleted(runId, output);
    await deps.storage.recordEvent({ runId, type: "completed", payload: null });
    return { status: "completed" };
  } catch (err) {
    if (isSuspend(err)) {
      return handleSuspend(deps, runId, err);
    }
    const error = toWorkflowError(err);
    try {
      await deps.storage.markFailed(runId, error);
      await deps.storage.recordEvent({ runId, type: "failed", payload: null });
    } catch (markErr) {
      deps.logger.error(markErr instanceof Error ? markErr : new Error(String(markErr)), {
        runId,
        event: "workflow.markFailed_failed",
      });
    }
    if (!(err instanceof WorkflowRuntimeError)) {
      deps.logger.error(err instanceof Error ? err : new Error(String(err)), {
        runId,
        event: "workflow.unhandled_error",
      });
    }
    return { status: "failed" };
  }
};

const handleSuspend = async (
  deps: RunnerDeps,
  runId: string,
  err: WorkflowSuspend,
): Promise<RunResult> => {
  await deps.storage.transaction(async (tx) => {
    await tx.lockRun(runId);
    if (err.wakeOn) {
      await tx.markWaiting(runId);
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
