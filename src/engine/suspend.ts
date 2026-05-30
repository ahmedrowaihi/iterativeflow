import type { SuspendReason } from "./types";

export interface SuspendOpts {
  wakeAt?: Date;
  wakeOn?: string;
  reason: SuspendReason;
}

export class FlowSuspend extends Error {
  readonly wakeAt?: Date;
  readonly wakeOn?: string;
  readonly reason: SuspendReason;

  constructor(opt: SuspendOpts) {
    super(`flow suspended (${opt.reason})`);
    this.name = "FlowSuspend";
    this.wakeAt = opt.wakeAt;
    this.wakeOn = opt.wakeOn;
    this.reason = opt.reason;
  }
}

export const isSuspend = (err: unknown): err is FlowSuspend => err instanceof FlowSuspend;
