import { toFireAt } from "../util/duration";
import { enforcePayloadCap } from "../util/payload-cap";
import { formatIssues, type StandardSchemaV1, validate } from "../util/standard-schema";
import type { ProgressWaiters } from "./progress-waiters";
import type { SchemaVersionCheck } from "./schema-version-check";
import type { TerminalWaiters } from "./terminal-waiters";
import type { FlowHandle, MetricsRecorder, StartOpts, Storage, WaitUntil } from "./types";

interface Deps {
  storage: Storage;
  metrics: MetricsRecorder;
  schemaCheck: SchemaVersionCheck;
  terminalWaiters: TerminalWaiters;
  progressWaiters: ProgressWaiters;
  ensureListen: () => void;
  maxInputBytes?: number;
}

const cursorKeyFor = (until: WaitUntil): { kind: "step" | "signal"; cursorKey: string } =>
  "step" in until
    ? { kind: "step", cursorKey: until.step }
    : { kind: "signal", cursorKey: `signal:${until.signal}` };

/**
 * Factory for the public-facing {@link FlowHandle}. One handle per registered
 * `(name, version)`. Owns `start` (idempotent insert + enqueue), `output`
 * (terminal-state read), and `result` (LISTEN-backed block until terminal).
 *
 * @internal
 */
export const createHandleFactory =
  (deps: Deps) =>
  <I, O>(
    name: string,
    version: number,
    inputSchema: StandardSchemaV1<unknown, I> | undefined,
  ): FlowHandle<I, O> => ({
    name,
    version,
    async start(input, startOpts: StartOpts = {}) {
      let validated: I = input;
      if (inputSchema) {
        const parsed = await validate(inputSchema, input);
        if (parsed.issues) {
          throw new Error(`Flow "${name}" input failed schema: ${formatIssues(parsed.issues)}`);
        }
        validated = parsed.value;
      }
      enforcePayloadCap(`Flow "${name}" input`, validated, deps.maxInputBytes);
      await deps.schemaCheck.ensure();
      const runAt = startOpts.delay ? toFireAt(startOpts.delay) : undefined;

      const result = await deps.storage.transaction(async (tx) => {
        const { runId, status, created } = await tx.createRun({
          name,
          version,
          input: validated,
          idempotencyKey: startOpts.idempotencyKey,
          tags: startOpts.tags,
        });
        if (created) {
          await tx.recordEvent({
            runId,
            type: "started",
            payload: { idempotent: false },
          });
          await tx.enqueue(runId, { runAt, priority: startOpts.priority });
        }
        return { runId, status, created };
      });
      if (result.created) deps.metrics.runStarted?.({ name, version });
      return { runId: result.runId, status: result.status };
    },
    output: (runId) => deps.storage.loadOutput(runId) as Promise<O | undefined>,
    async result(runId, resultOpts) {
      const existing = await deps.storage.loadRunDetail(runId);
      if (!existing) throw new Error(`run ${runId} not found`);
      if (existing.run.status === "done") return existing.run.output as O;
      if (existing.run.status === "failed" || existing.run.status === "canceled") {
        const err = existing.run.error ?? {
          code: existing.run.status === "canceled" ? "RUN_CANCELED" : "FLOW_UNKNOWN",
          message: `run ${runId} ended ${existing.run.status}`,
        };
        throw new Error(`flow "${name}" ${existing.run.status}: ${err.message}`);
      }
      deps.ensureListen();
      const pending = deps.terminalWaiters.wait(runId, resultOpts?.timeoutMs);
      const recheck = await deps.storage.loadRunDetail(runId);
      if (
        recheck &&
        (recheck.run.status === "done" ||
          recheck.run.status === "failed" ||
          recheck.run.status === "canceled")
      ) {
        deps.terminalWaiters.notify(runId);
      }
      await pending;
      const final = await deps.storage.loadRunDetail(runId);
      if (!final) throw new Error(`run ${runId} disappeared after notify`);
      if (final.run.status === "done") return final.run.output as O;
      const err = final.run.error ?? {
        code: final.run.status === "canceled" ? "RUN_CANCELED" : "FLOW_UNKNOWN",
        message: `run ${runId} ended ${final.run.status}`,
      };
      throw new Error(`flow "${name}" ${final.run.status}: ${err.message}`);
    },
    async wait(runId, waitOpts) {
      const { kind, cursorKey } = cursorKeyFor(waitOpts.until);
      deps.ensureListen();
      const pending = deps.progressWaiters.wait(runId, kind, cursorKey, {
        timeoutMs: waitOpts.timeoutMs,
      });
      if (kind === "step") {
        const row = await deps.storage.loadStep(runId, cursorKey);
        if (row && (row.status === "ok" || row.status === "failed_terminal")) {
          deps.progressWaiters.notify(runId, kind, cursorKey);
        }
      } else {
        const row = await deps.storage.loadSignal(runId, cursorKey);
        if (row?.delivered) {
          deps.progressWaiters.notify(runId, kind, cursorKey);
        }
      }
      return pending;
    },
  });
