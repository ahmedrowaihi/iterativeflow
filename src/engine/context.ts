import { computeBackoff } from "../util/backoff";
import { type Duration, toFireAt, toMs } from "../util/duration";
import { toWorkflowError, WorkflowRuntimeError } from "../util/errors";
import { formatIssues, validate } from "../util/standard-schema";
import { isSuspend, WorkflowSuspend } from "./suspend";
import type { HookOpts, Logger, RunSnapshot, StepOpts, Storage, WorkflowContext } from "./types";

const runWithTimeout = async <T>(
  fn: () => Promise<T> | T,
  timeoutMs: number | undefined,
  stepKey: string,
): Promise<T> => {
  const work = Promise.resolve().then(fn);
  if (!timeoutMs || timeoutMs <= 0) return work;
  let timer: ReturnType<typeof setTimeout> | undefined;
  const timeout = new Promise<never>((_, reject) => {
    timer = setTimeout(
      () => reject(new Error(`step "${stepKey}" exceeded timeoutMs=${timeoutMs}`)),
      timeoutMs,
    );
  });
  try {
    return await Promise.race([work, timeout]);
  } finally {
    if (timer) clearTimeout(timer);
  }
};

interface ContextDeps {
  runId: string;
  attempt: number;
  storage: Storage;
  snapshot: RunSnapshot;
  logger: Logger;
}

export class RuntimeWorkflowContext implements WorkflowContext {
  readonly runId: string;
  readonly attempt: number;
  private readonly storage: Storage;
  private readonly snapshot: RunSnapshot;
  private readonly logger: Logger;
  private readonly cursors = new Map<string, number>();

  constructor(deps: ContextDeps) {
    this.runId = deps.runId;
    this.attempt = deps.attempt;
    this.storage = deps.storage;
    this.snapshot = deps.snapshot;
    this.logger = deps.logger;
  }

  async step<T>(name: string, fn: () => Promise<T> | T, opts: StepOpts = {}): Promise<T> {
    const stepKey = this.nextKey(name);
    const existing = this.snapshot.steps.get(stepKey);

    if (existing?.status === "ok") return existing.result as T;
    if (existing?.status === "failed_terminal") {
      const e = existing.error ?? {
        code: "STEP_FAILED",
        message: "step previously failed",
      };
      throw new WorkflowRuntimeError({
        code: e.code,
        message: e.message,
        nonRetryable: true,
      });
    }

    const attempts = (existing?.attempts ?? 0) + 1;
    const maxAttempts = (opts.retries ?? 3) + 1;

    await this.storage.startStep(this.runId, stepKey, attempts);
    await this.storage.recordEvent({
      runId: this.runId,
      type: "step_started",
      stepKey,
      payload: { attempts },
    });

    let fnResult: T;
    let fnError: unknown = null;
    try {
      fnResult = await runWithTimeout(fn, opts.timeoutMs, stepKey);
    } catch (err) {
      if (isSuspend(err)) {
        throw new WorkflowRuntimeError({
          code: "WORKFLOW_SUSPEND_IN_STEP",
          message: `ctx.sleep / ctx.hook cannot be called inside ctx.step("${stepKey}"). Move them to top-level workflow code.`,
          nonRetryable: true,
        });
      }
      fnError = err;
    }

    if (fnError === null) {
      const result = fnResult!;
      await this.storage.finishStep({
        runId: this.runId,
        stepKey,
        status: "ok",
        result,
        attempts,
      });
      await this.storage.recordEvent({
        runId: this.runId,
        type: "step_ok",
        stepKey,
        payload: { attempts },
      });
      this.snapshot.steps.set(stepKey, {
        runId: this.runId,
        stepKey,
        status: "ok",
        result,
        error: null,
        attempts,
        startedAt: existing?.startedAt ?? new Date(),
        completedAt: new Date(),
      });
      return result;
    }

    const error = toWorkflowError(fnError);
    const isNonRetryable = fnError instanceof WorkflowRuntimeError && fnError.nonRetryable;
    const classification = opts.classify
      ? opts.classify(fnError instanceof Error ? fnError : new Error(String(fnError)))
      : "transient";
    const exhausted = attempts >= maxAttempts;
    const terminal = isNonRetryable || classification === "permanent" || exhausted;

    if (terminal) {
      await this.storage.finishStep({
        runId: this.runId,
        stepKey,
        status: "failed_terminal",
        error,
        attempts,
      });
      await this.storage.recordEvent({
        runId: this.runId,
        type: "step_terminal",
        stepKey,
        payload: { code: error.code, attempts },
      });
      throw new WorkflowRuntimeError({ ...error, nonRetryable: true });
    }

    await this.storage.finishStep({
      runId: this.runId,
      stepKey,
      status: "failed_retry",
      error,
      attempts,
    });
    await this.storage.recordEvent({
      runId: this.runId,
      type: "step_failed",
      stepKey,
      payload: { code: error.code, attempts },
    });

    const wakeAt = new Date(
      Date.now() +
        computeBackoff(attempts, {
          policy: opts.backoff,
          baseMs: opts.baseBackoffMs,
          capMs: opts.capBackoffMs,
        }),
    );
    throw new WorkflowSuspend({ reason: "step_retry", wakeAt });
  }

  async sleep(duration: Duration): Promise<void> {
    const stepKey = this.nextKey("sleep");
    const existing = this.snapshot.timers.get(stepKey);

    if (existing?.firedAt) return;

    const fireAt = existing?.fireAt ?? toFireAt(duration);
    if (!existing) {
      await this.storage.createTimer(this.runId, stepKey, fireAt);
      await this.storage.recordEvent({
        runId: this.runId,
        type: "sleep_scheduled",
        stepKey,
        payload: { fireAt },
      });
    }

    if (fireAt <= new Date()) {
      await this.storage.fireTimer(this.runId, stepKey);
      await this.storage.recordEvent({
        runId: this.runId,
        type: "sleep_fired",
        stepKey,
      });
      this.snapshot.timers.set(stepKey, {
        runId: this.runId,
        stepKey,
        fireAt,
        firedAt: new Date(),
      });
      return;
    }

    throw new WorkflowSuspend({ reason: "sleep", wakeAt: fireAt });
  }

  async hook<T = unknown>(name: string, opts: HookOpts<T> = {}): Promise<T> {
    const hookKey = this.nextKey(`hook:${name}`);
    const existing = this.snapshot.hooks.get(hookKey);

    if (existing?.delivered) {
      const payload = existing.payload;
      if (opts.schema) {
        const parsed = await validate(opts.schema, payload);
        if (parsed.issues) {
          throw new WorkflowRuntimeError({
            code: "HOOK_PAYLOAD_INVALID",
            message: `Hook ${name} payload failed schema: ${formatIssues(parsed.issues)}`,
            nonRetryable: true,
          });
        }
        return parsed.value;
      }
      return payload as T;
    }

    if (existing?.expiresAt && existing.expiresAt <= new Date()) {
      await this.storage.recordEvent({
        runId: this.runId,
        type: "hook_timeout",
        stepKey: hookKey,
      });
      throw new WorkflowRuntimeError({
        code: "WORKFLOW_HOOK_TIMEOUT",
        message: `Hook ${name} expired`,
        nonRetryable: true,
      });
    }

    if (!existing) {
      const expiresAt = opts.timeout ? new Date(Date.now() + toMs(opts.timeout)) : undefined;

      const result = await this.storage.armOrConsumeHook(this.runId, hookKey, expiresAt);

      if (result.kind === "consumed") {
        this.snapshot.hooks.set(hookKey, {
          runId: this.runId,
          hookKey,
          delivered: true,
          payload: result.payload,
          expiresAt: null,
          createdAt: new Date(),
          deliveredAt: new Date(),
        });
        if (opts.schema) {
          const parsed = await validate(opts.schema, result.payload);
          if (parsed.issues) {
            throw new WorkflowRuntimeError({
              code: "HOOK_PAYLOAD_INVALID",
              message: `Hook ${name} payload failed schema: ${formatIssues(parsed.issues)}`,
              nonRetryable: true,
            });
          }
          return parsed.value;
        }
        return result.payload as T;
      }

      throw new WorkflowSuspend({
        reason: "hook",
        wakeOn: hookKey,
        wakeAt: expiresAt,
      });
    }

    throw new WorkflowSuspend({
      reason: "hook",
      wakeOn: hookKey,
      wakeAt: existing.expiresAt ?? undefined,
    });
  }

  log(message: string, payload?: Record<string, unknown>): void {
    this.logger.info(message, { runId: this.runId, ...payload });
  }

  /**
   * Each named call increments its own cursor — first is `name`, then
   * `name:1`, `name:2`. Stable across replays because workflow code is
   * deterministic.
   */
  private nextKey(name: string): string {
    const idx = this.cursors.get(name) ?? 0;
    this.cursors.set(name, idx + 1);
    return idx === 0 ? name : `${name}:${idx}`;
  }
}
