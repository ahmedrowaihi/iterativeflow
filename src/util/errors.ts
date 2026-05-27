import type { WorkflowError } from "../storage/schema";

export class WorkflowRuntimeError extends Error {
  readonly code: string;
  readonly nonRetryable: boolean;

  constructor(opt: { code: string; message: string; nonRetryable?: boolean }) {
    super(opt.message);
    this.name = "WorkflowRuntimeError";
    this.code = opt.code;
    this.nonRetryable = opt.nonRetryable ?? false;
  }
}

export const toWorkflowError = (err: unknown): WorkflowError => {
  if (err instanceof WorkflowRuntimeError) {
    return { code: err.code, message: err.message, stack: err.stack };
  }
  if (err instanceof Error) {
    const code = (err as Error & { code?: string }).code ?? err.name ?? "UNKNOWN";
    return { code, message: err.message, stack: err.stack };
  }
  return { code: "UNKNOWN", message: String(err) };
};
