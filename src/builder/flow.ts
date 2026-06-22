import type { FlowContext, SignalOpts, StepArg, StepOpts } from "../engine/types";
import type { Duration } from "../util/duration";
import type { StandardSchemaV1 } from "../util/standard-schema";
import type { FlowContract } from "./contract";
import type { FlowDefinition, FlowNode, StepNode } from "./types";

interface FlowState {
  readonly name: string;
  readonly version: number;
  readonly input?: StandardSchemaV1<unknown, unknown>;
  readonly nodes: ReadonlyArray<FlowNode>;
  readonly outputFn?: (arg: { input: unknown }) => unknown;
}

const collectSignalSchemas = (
  nodes: ReadonlyArray<FlowNode>,
  out: Map<string, StandardSchemaV1<unknown, unknown>>,
): void => {
  for (const node of nodes) {
    if (node.kind === "signal" && node.opts?.schema) {
      const existing = out.get(node.name);
      const schema = node.opts.schema as StandardSchemaV1<unknown, unknown>;
      if (existing && existing !== schema) {
        throw new Error(
          `flow signal "${node.name}" declared with two different schemas; collapse to one declaration`,
        );
      }
      out.set(node.name, schema);
    } else if (node.kind === "loop") {
      collectSignalSchemas(node.nodes, out);
    }
  }
};

const finalize = <I, O>(state: FlowState): FlowDefinition<I, O> => {
  const nodes = state.nodes.slice();
  const outputFn = state.outputFn;
  const schemas = new Map<string, StandardSchemaV1<unknown, unknown>>();
  collectSignalSchemas(nodes, schemas);

  const body = async (ctx: FlowContext, input: I): Promise<O> => {
    let channel: unknown = input;

    const exec = async (xs: ReadonlyArray<FlowNode>): Promise<void> => {
      for (const node of xs) {
        if (node.kind === "step") {
          const current = channel;
          channel = await ctx.step(
            node.name,
            (arg: StepArg) => node.fn({ ...arg, input: current }),
            node.opts,
          );
        } else if (node.kind === "sleep") {
          await ctx.sleep(node.duration);
        } else if (node.kind === "signal") {
          const payload = await ctx.signal(node.name, node.opts);
          channel = node.merge ? node.merge(channel, payload) : payload;
        } else {
          while (!node.until(channel)) {
            await exec(node.nodes);
          }
        }
      }
    };

    await exec(nodes);
    return (outputFn ? outputFn({ input: channel }) : channel) as O;
  };

  return {
    name: state.name,
    version: state.version,
    input: state.input as StandardSchemaV1<unknown, I> | undefined,
    nodes,
    body,
    signalSchemas: schemas.size > 0 ? schemas : undefined,
  };
};

/**
 * Returned by `.output(...)` — the only continuation it supports is `build()`,
 * which finalizes a flow whose output type is the value returned by `output`.
 */
class TerminalFlowBuilder<I, O> {
  /** @internal */ constructor(private readonly state: FlowState) {}
  /** Finalize and return a {@link FlowDefinition} for `engine.register(...)`. */
  build(): FlowDefinition<I, O> {
    return finalize<I, O>(this.state);
  }
}

/**
 * Fluent flow builder. Every method returns a NEW instance so chains never
 * mutate one another. `Channel` tracks the value threaded between steps; `Out`
 * is the contract-imposed upper bound on `.output(...)` (`unknown` — no bound —
 * for `flow(name)`; the contract's output type for `flow(contract)`).
 */
class FlowBuilder<I, Channel, Out = unknown> {
  /** @internal */ constructor(private readonly state: FlowState) {}

  private withState<NewI, NewChannel>(state: FlowState): FlowBuilder<NewI, NewChannel, Out> {
    return new FlowBuilder<NewI, NewChannel, Out>(state);
  }

  private append<NewChannel>(node: FlowNode): FlowBuilder<I, NewChannel, Out> {
    return this.withState<I, NewChannel>({ ...this.state, nodes: [...this.state.nodes, node] });
  }

  /** Set the schema version. Bump when you reshape the body; must not regress. */
  version(version: number): FlowBuilder<I, Channel, Out> {
    if (!Number.isInteger(version) || version < 1) {
      throw new Error(
        `flow("${this.state.name}").version: must be a positive integer, got ${version}`,
      );
    }
    if (version < this.state.version) {
      throw new Error(
        `flow("${this.state.name}").version: must not regress (${this.state.version} → ${version})`,
      );
    }
    return this.withState<I, Channel>({ ...this.state, version });
  }

  /** Attach a Standard Schema validator for the run input. Resets the channel type to the input. */
  input<I2>(schema: StandardSchemaV1<unknown, I2>): FlowBuilder<I2, I2, Out> {
    return this.withState<I2, I2>({
      ...this.state,
      input: schema as StandardSchemaV1<unknown, unknown>,
    });
  }

  /** Append a memoized step. Its return value becomes the next channel value. */
  step<T>(
    name: string,
    fn: (arg: StepArg<Channel>) => T | Promise<T>,
    opts?: StepOpts,
  ): FlowBuilder<I, Awaited<T>, Out> {
    return this.append<Awaited<T>>({
      kind: "step",
      name,
      fn: fn as StepNode["fn"],
      opts,
    });
  }

  /** Append a sleep — pauses the run until `duration` elapses. Channel is unchanged. */
  sleep(duration: Duration): FlowBuilder<I, Channel, Out> {
    return this.append<Channel>({ kind: "sleep", duration });
  }

  /** Append a loop. The sub-builder's chain is executed until `opts.until(channel)` is `true`. */
  loop(
    opts: { until: (input: Channel) => boolean },
    body: (sub: FlowBuilder<I, Channel>) => FlowBuilder<I, Channel>,
  ): FlowBuilder<I, Channel, Out> {
    const seed: FlowState = {
      name: this.state.name,
      version: this.state.version,
      nodes: [],
    };
    const sub = body(new FlowBuilder<I, Channel>(seed));
    return this.append<Channel>({
      kind: "loop",
      until: opts.until as (input: unknown) => boolean,
      nodes: sub.state.nodes,
    });
  }

  /** Append a signal await. The delivered payload becomes the next channel value. */
  signal<T>(name: string, opts?: SignalOpts<T>): FlowBuilder<I, T, Out>;
  /** Append a signal await with a custom `merge` that combines the channel and payload. */
  signal<T, R>(
    name: string,
    opts: SignalOpts<T>,
    merge: (input: Channel, payload: T) => R,
  ): FlowBuilder<I, R, Out>;
  signal(
    name: string,
    /* eslint-disable @typescript-eslint/no-explicit-any */
    opts?: SignalOpts<any>,
    merge?: (input: any, payload: any) => any,
  ): FlowBuilder<I, any, Out> {
    /* eslint-enable @typescript-eslint/no-explicit-any */
    return this.append({
      kind: "signal",
      name,
      opts: opts as SignalOpts<unknown> | undefined,
      merge: merge as ((input: unknown, payload: unknown) => unknown) | undefined,
    });
  }

  /**
   * Set a final transform run on the channel value before the run completes.
   * For a contract-seeded flow the returned value is constrained to the
   * contract's output type (`O2 extends Out`); for `flow(name)` it is inferred.
   */
  output<O2 extends Out>(fn: (arg: { input: Channel }) => O2): TerminalFlowBuilder<I, O2> {
    return new TerminalFlowBuilder<I, O2>({
      ...this.state,
      outputFn: fn as FlowState["outputFn"],
    });
  }

  /** Finalize and return a {@link FlowDefinition} for `engine.register(...)`. */
  build(): FlowDefinition<I, Channel> {
    return finalize<I, Channel>(this.state);
  }
}

/**
 * Start a new flow builder. Each chaining call returns a NEW builder instance —
 * branches don't share state, so `const a = flow("x"); const b = a.step(...);`
 * leaves `a` untouched.
 *
 * Pass a {@link FlowContract} instead of a name to seed `name`/`version`/`input`
 * from the contract and constrain `.output(...)` to the contract's output type —
 * the worker's body then cannot drift from the enqueue-only `.start` callers.
 */
export function flow(name: string): FlowBuilder<unknown, undefined, unknown>;
/** Seed a builder from a {@link FlowContract} — pins `name`/`version`/`input` and constrains `.output(...)` to the contract's output type. */
export function flow<I, O>(contract: FlowContract<I, O>): FlowBuilder<I, I, O>;
export function flow(
  arg: string | FlowContract<unknown, unknown>,
): FlowBuilder<unknown, unknown, unknown> {
  if (typeof arg === "string") {
    return new FlowBuilder({ name: arg, version: 1, nodes: [] });
  }
  return new FlowBuilder({
    name: arg.name,
    version: arg.version,
    input: arg.input as StandardSchemaV1<unknown, unknown> | undefined,
    nodes: [],
  });
}

export type { FlowBuilder, TerminalFlowBuilder };
