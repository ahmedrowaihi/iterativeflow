import { toFireAt } from "../util/duration";
import { enforcePayloadCap } from "../util/payload-cap";
import { formatIssues, type StandardSchemaV1, validate } from "../util/standard-schema";
import type { ProgressWaiters } from "./progress-waiters";
import type { SchemaVersionCheck } from "./schema-version-check";
import type { TerminalWaiters } from "./terminal-waiters";
import type {
  FlowHandle,
  MetricsRecorder,
  StartManyItem,
  StartOpts,
  StartRunSpec,
  Storage,
  WaitUntil,
} from "./types";

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
  ): FlowHandle<I, O> => {
    const specFor = async (item: StartManyItem<I>): Promise<StartRunSpec> => {
      let validated: I = item.input;
      if (inputSchema) {
        const parsed = await validate(inputSchema, item.input);
        if (parsed.issues) {
          throw new Error(`Flow "${name}" input failed schema: ${formatIssues(parsed.issues)}`);
        }
        validated = parsed.value;
      }
      enforcePayloadCap(`Flow "${name}" input`, validated, deps.maxInputBytes);
      return {
        name,
        version,
        input: validated,
        idempotencyKey: item.idempotencyKey,
        tags: item.tags,
        runAt: item.delay ? toFireAt(item.delay) : undefined,
        priority: item.priority,
      };
    };

    return {
      name,
      version,
      async start(input, startOpts: StartOpts = {}) {
        const spec = await specFor({ input, ...startOpts });
        await deps.schemaCheck.ensure();
        const result = await deps.storage.startRun(spec, startOpts.tx);
        if (result.created) deps.metrics.runStarted?.({ name, version });
        return { runId: result.runId, status: result.status };
      },
      async startMany(items, batchOpts) {
        const specs: StartRunSpec[] = [];
        for (const item of items) specs.push(await specFor(item));
        await deps.schemaCheck.ensure();
        const results = await deps.storage.startManyRuns(specs, batchOpts?.tx);
        for (const r of results) if (r.created) deps.metrics.runStarted?.({ name, version });
        return results.map((r) => ({ runId: r.runId, status: r.status }));
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
    };
  };
