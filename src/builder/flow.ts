import type { HookOpts, StepOpts, WorkflowContext } from "../engine/types";
import type { Duration } from "../util/duration";
import type { StandardSchemaV1 } from "../util/standard-schema";
import type { FlowDefinition, FlowNode, NodeArg, NodeCtx, StepNode } from "./types";

interface FlowState {
  name: string;
  version: number;
  input?: StandardSchemaV1<unknown, unknown>;
  nodes: FlowNode[];
  outputFn?: (arg: NodeArg<unknown>) => unknown;
}

const finalize = <I, O>(state: FlowState): FlowDefinition<I, O> => {
  const nodes = state.nodes.slice();
  const outputFn = state.outputFn;

  const run = async (ctx: WorkflowContext, input: I): Promise<O> => {
    const nodeCtx: NodeCtx = {
      runId: ctx.runId,
      attempt: ctx.attempt,
      log: (message, payload) => ctx.log(message, payload),
    };

    let channel: unknown = input;

    const exec = async (xs: ReadonlyArray<FlowNode>): Promise<void> => {
      for (const node of xs) {
        if (node.kind === "step") {
          const current = channel;
          channel = await ctx.step(
            node.name,
            () => node.fn({ ctx: nodeCtx, input: current }),
            node.opts,
          );
        } else if (node.kind === "sleep") {
          await ctx.sleep(node.duration);
        } else if (node.kind === "hook") {
          const payload = await ctx.hook(node.name, node.opts);
          channel = node.merge ? node.merge(channel, payload) : payload;
        } else {
          while (!node.until(channel)) {
            await exec(node.nodes);
          }
        }
      }
    };

    await exec(nodes);
    return (outputFn ? outputFn({ ctx: nodeCtx, input: channel }) : channel) as O;
  };

  return {
    name: state.name,
    version: state.version,
    input: state.input as StandardSchemaV1<unknown, I> | undefined,
    nodes,
    run,
  };
};

class TerminalFlowBuilder<I, O> {
  constructor(private readonly state: FlowState) {}

  build(): FlowDefinition<I, O> {
    return finalize<I, O>(this.state);
  }
}

class FlowBuilder<I, Channel> {
  constructor(private readonly state: FlowState) {}

  version(version: number): FlowBuilder<I, Channel> {
    this.state.version = version;
    return this;
  }

  input<I2>(schema: StandardSchemaV1<unknown, I2>): FlowBuilder<I2, I2> {
    this.state.input = schema as StandardSchemaV1<unknown, unknown>;
    return this as unknown as FlowBuilder<I2, I2>;
  }

  step<T>(
    name: string,
    fn: (arg: NodeArg<Channel>) => T | Promise<T>,
    opts?: StepOpts,
  ): FlowBuilder<I, Awaited<T>> {
    this.state.nodes.push({
      kind: "step",
      name,
      fn: fn as StepNode["fn"],
      opts,
    });
    return this as unknown as FlowBuilder<I, Awaited<T>>;
  }

  sleep(duration: Duration): FlowBuilder<I, Channel> {
    this.state.nodes.push({ kind: "sleep", duration });
    return this;
  }

  loop(
    opts: { until: (input: Channel) => boolean },
    body: (sub: FlowBuilder<I, Channel>) => FlowBuilder<I, Channel>,
  ): FlowBuilder<I, Channel> {
    const subState: FlowState = {
      name: this.state.name,
      version: this.state.version,
      nodes: [],
    };
    body(new FlowBuilder<I, Channel>(subState));
    this.state.nodes.push({
      kind: "loop",
      until: opts.until as (input: unknown) => boolean,
      nodes: subState.nodes,
    });
    return this;
  }

  hook<T>(name: string, opts?: HookOpts<T>): FlowBuilder<I, T>;
  hook<T, R>(
    name: string,
    opts: HookOpts<T>,
    merge: (input: Channel, payload: T) => R,
  ): FlowBuilder<I, R>;
  hook(
    name: string,
    /* eslint-disable @typescript-eslint/no-explicit-any */
    opts?: HookOpts<any>,
    merge?: (input: any, payload: any) => any,
  ): FlowBuilder<I, any> {
    /* eslint-enable @typescript-eslint/no-explicit-any */
    this.state.nodes.push({
      kind: "hook",
      name,
      opts: opts as HookOpts<unknown> | undefined,
      merge: merge as ((input: unknown, payload: unknown) => unknown) | undefined,
    });
    return this;
  }

  output<O>(fn: (arg: NodeArg<Channel>) => O): TerminalFlowBuilder<I, O> {
    this.state.outputFn = fn as FlowState["outputFn"];
    return new TerminalFlowBuilder<I, O>(this.state);
  }

  build(): FlowDefinition<I, Channel> {
    return finalize<I, Channel>(this.state);
  }
}

export const flow = (name: string): FlowBuilder<unknown, undefined> =>
  new FlowBuilder<unknown, undefined>({ name, version: 1, nodes: [] });

export type { FlowBuilder, TerminalFlowBuilder };
