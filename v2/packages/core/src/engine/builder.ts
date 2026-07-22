import type { Ctx, StepPolicy } from "#engine/context";
import type { Flow } from "#engine/flow";

type Acc<I> = { input: I };

interface StepDef {
  name: string;
  fn: (acc: Record<string, unknown>, ctx: Ctx) => unknown;
  policy?: StepPolicy;
}

/**
 * Fluent, fully-typed flow builder. It compiles down to the SAME `Flow.run` + `ctx.step` the
 * imperative `defineFlow` uses — the builder is sugar for wiring typed steps, not a second
 * execution path. Steps run in declared order; their names become the accumulator keys.
 */
export class FlowBuilder<I, A extends Acc<I>> {
  constructor(
    private readonly flowName: string,
    private readonly version: number,
    private readonly steps: readonly StepDef[],
  ) {}

  /** Append a named step. Its result is added to the accumulator under `name` for later steps. */
  step<N extends string, R>(
    name: N,
    fn: (acc: A, ctx: Ctx) => R | Promise<R>,
    policy?: StepPolicy,
  ): FlowBuilder<I, A & { [K in N]: R }> {
    const def: StepDef = {
      name,
      fn: fn as StepDef["fn"],
      policy,
    };
    return new FlowBuilder(this.flowName, this.version, [...this.steps, def]);
  }

  /** Finalize with an explicit output projection over the full accumulator. */
  output<O>(fn: (acc: A, ctx: Ctx) => O | Promise<O>): Flow<I, O> {
    const steps = this.steps;
    return {
      name: this.flowName,
      version: this.version,
      run: async (ctx, input) => {
        const acc: Record<string, unknown> = { input };
        for (const s of steps) {
          acc[s.name] = await ctx.step(s.name, () => s.fn(acc, ctx), s.policy);
        }
        return fn(acc as A, ctx);
      },
    };
  }

  /** Finalize with the whole accumulator as the output. */
  build(): Flow<I, A> {
    return this.output((acc) => acc);
  }
}

/** Start a typed flow builder. Reserve the accumulator key `input` — it holds the run input. */
export const builder = <I>(name: string, version: number): FlowBuilder<I, Acc<I>> =>
  new FlowBuilder(name, version, []);
