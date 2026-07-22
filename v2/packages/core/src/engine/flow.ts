import type { Ctx } from "#engine/context";

/** The Standard Schema calling surface (spec v1) — zod/valibot/arktype schemas all satisfy it. */
export interface InputSchema<I> {
  readonly "~standard": {
    readonly version: 1;
    readonly vendor: string;
    readonly validate: (
      value: unknown,
    ) =>
      | { value: I; issues?: undefined }
      | { issues: ReadonlyArray<{ message: string }> }
      | Promise<{ value: I; issues?: undefined } | { issues: ReadonlyArray<{ message: string }> }>;
  };
}

/**
 * A durable flow: a named, versioned function whose body is deterministic between the
 * `ctx` calls (steps, sleeps, invokes). The executor may re-invoke it any number of times
 * (crash recovery, wake-from-sleep); memoized `ctx` calls short-circuit so only un-run work
 * executes. Non-determinism BETWEEN ctx calls (Date.now, random, branching on wall-clock)
 * is the one footgun — do that work inside `ctx.step` so its result is memoized.
 */
export interface Flow<I = unknown, O = unknown> {
  name: string;
  version: number;
  run: (ctx: Ctx, input: I) => Promise<O>;
  /** Optional Standard-Schema validator for the input, checked at submit time. */
  input?: InputSchema<I>;
}

/** Validate `input` against a flow's schema (if any). Throws with the collected issues. */
export const validateInput = async <I>(flow: Flow<I, unknown>, input: I): Promise<I> => {
  if (!flow.input) return input;
  const r = await flow.input["~standard"].validate(input);
  if (r.issues) {
    throw new Error(
      `input validation failed for ${flow.name}: ${r.issues.map((i) => i.message).join("; ")}`,
    );
  }
  return r.value;
};

/** Define a durable flow. Ships alongside the builder API; both produce a {@link Flow}. */
export const defineFlow = <I, O>(flow: Flow<I, O>): Flow<I, O> => flow;

/** A flow of any shape — the registry and executor dispatch flows type-erased. */
export type AnyFlow = Flow<any, any>;

/** A registry the executor resolves a run's `(name, version)` against to its {@link Flow}. */
export type FlowRegistry = ReadonlyMap<string, AnyFlow>;

/** @internal */
export const flowKey = (name: string, version: number): string => `${name}@${version}`;

/** Build a {@link FlowRegistry} from a list of flows. */
export const registry = (flows: readonly AnyFlow[]): FlowRegistry =>
  new Map(flows.map((f) => [flowKey(f.name, f.version), f]));
