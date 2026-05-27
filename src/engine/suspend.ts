export interface SuspendOpts {
  wakeAt?: Date;
  wakeOn?: string;
  reason: "sleep" | "hook" | "step_retry";
}

export class WorkflowSuspend extends Error {
  readonly wakeAt?: Date;
  readonly wakeOn?: string;
  readonly reason: SuspendOpts["reason"];

  constructor(opt: SuspendOpts) {
    super(`workflow suspended (${opt.reason})`);
    this.name = "WorkflowSuspend";
    this.wakeAt = opt.wakeAt;
    this.wakeOn = opt.wakeOn;
    this.reason = opt.reason;
  }
}

export const isSuspend = (err: unknown): err is WorkflowSuspend => err instanceof WorkflowSuspend;
