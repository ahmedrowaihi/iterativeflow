import type { HookOpts, StepOpts, WorkflowContext } from "../engine/types";
import type { Duration } from "../util/duration";
import type { StandardSchemaV1 } from "../util/standard-schema";

export interface NodeCtx {
  readonly runId: string;
  readonly attempt: number;
  log(message: string, payload?: Record<string, unknown>): void;
}

export interface NodeArg<Input> {
  readonly ctx: NodeCtx;
  readonly input: Input;
}

export interface StepNode {
  kind: "step";
  name: string;
  fn: (arg: NodeArg<unknown>) => unknown;
  opts?: StepOpts;
}

export interface SleepNode {
  kind: "sleep";
  duration: Duration;
}

export interface HookNode {
  kind: "hook";
  name: string;
  opts?: HookOpts<unknown>;
  merge?: (input: unknown, payload: unknown) => unknown;
}

export interface LoopNode {
  kind: "loop";
  until: (input: unknown) => boolean;
  nodes: ReadonlyArray<FlowNode>;
}

export type FlowNode = StepNode | SleepNode | HookNode | LoopNode;

export interface FlowDefinition<I, O> {
  readonly name: string;
  readonly version: number;
  readonly input?: StandardSchemaV1<unknown, I>;
  readonly nodes: ReadonlyArray<FlowNode>;
  readonly run: (ctx: WorkflowContext, input: I) => Promise<O>;
}
