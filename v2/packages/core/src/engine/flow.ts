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

/** A flow's signal contract: signal name → payload type. Threads typed send + await. */
export type SignalMap = Record<string, unknown>;

/** No declared signals — the default. `ctx.signal(name)` falls back to the untyped overload. */
export type NoSignals = Record<never, never>;

/**
 * A phantom marker carrying a signal's payload type — declare it in a flow's `signals` map to
 * type both `ctx.signal(name)` (await) and `engine.signal(handle, name, payload)` (send). It holds
 * no value and is never read at runtime; it exists only to carry `T` into the type system.
 */
export interface SignalType<T> {
  readonly __payload?: T;
}

/** Declare a signal's payload type: `signals: { approve: type<{ by: string }>() }`. */
export const type = <T>(): SignalType<T> => ({});

/** The `signals` field's shape for a given map — one {@link SignalType} marker per name. */
export type SignalSchemas<S extends SignalMap> = { [K in keyof S]: SignalType<S[K]> };

/** Valid signal names for a map: the declared keys, or any string when none are declared. */
export type SignalName<S extends SignalMap> = [keyof S] extends [never] ? string : keyof S & string;

/** The payload type for signal `K` in map `S` — the declared type, or `unknown` when undeclared. */
export type SignalPayload<S extends SignalMap, K> = K extends keyof S ? S[K] : unknown;

/**
 * A durable flow: a named, versioned function whose body is deterministic between the
 * `ctx` calls (steps, sleeps, invokes). The executor may re-invoke it any number of times
 * (crash recovery, wake-from-sleep); memoized `ctx` calls short-circuit so only un-run work
 * executes. Non-determinism BETWEEN ctx calls (Date.now, random, branching on wall-clock)
 * is the one footgun — do that work inside `ctx.step` so its result is memoized.
 */
export interface Flow<I = unknown, O = unknown, S extends SignalMap = NoSignals> {
  name: string;
  version: number;
  run: (ctx: Ctx<S>, input: I) => Promise<O>;
  /** Optional Standard-Schema validator for the input, checked at submit time. */
  input?: InputSchema<I>;
  /**
   * Declares the signals this flow awaits (name → payload type). Type-only: it drives typed
   * `ctx.signal` / `engine.signal` and is never read at runtime. Build it with {@link type}.
   */
  signals?: SignalSchemas<S>;
}

/** Validate `input` against a flow's schema (if any). Throws with the collected issues. */
export const validateInput = async <I>(flow: Flow<I, any, any>, input: I): Promise<I> => {
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
export const defineFlow = <I, O, S extends SignalMap = NoSignals>(
  flow: Flow<I, O, S>,
): Flow<I, O, S> => flow;

/** A flow of any shape — the registry and executor dispatch flows type-erased. */
export type AnyFlow = Flow<any, any, any>;

/** A registry the executor resolves a run's `(name, version)` against to its {@link Flow}. */
export type FlowRegistry = ReadonlyMap<string, AnyFlow>;

/** @internal */
export const flowKey = (name: string, version: number): string => `${name}@${version}`;

/** Build a {@link FlowRegistry} from a list of flows. */
export const registry = (flows: readonly AnyFlow[]): FlowRegistry =>
  new Map(flows.map((f) => [flowKey(f.name, f.version), f]));
