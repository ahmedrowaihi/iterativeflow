import type { DriftPolicy } from "#types";
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

/** No declared signals — the default. `ctx.signal(name)` then takes any name, payload `unknown`. */
export type NoSignals = Record<never, never>;

/**
 * A signal's payload contract — any Standard-Schema validator (zod / valibot / arktype), exactly
 * like a flow's `input`. Declared in a flow's `signals` map, it types both `ctx.signal(name)` (await)
 * and `engine.signal(handle, name, payload)` (send), AND validates the payload when the flow consumes
 * it. Use {@link signalType} when you want the type without runtime validation.
 */
export type SignalSchema<T> = InputSchema<T>;

/**
 * Declare a signal's payload type WITHOUT runtime validation: `signals: { approve: signalType<{ by: string }>() }`.
 * Returns a Standard-Schema identity validator (accepts any value), so it slots into the same
 * `signals` map as a real zod/valibot schema — reach for a real schema when you want the payload checked.
 */
export const signalType = <T>(): SignalSchema<T> => ({
  "~standard": {
    version: 1,
    vendor: "iterativeflow",
    validate: (value) => ({ value: value as T }),
  },
});

/** The `signals` field's shape for a given map — one Standard-Schema validator per name. */
export type SignalSchemas<S extends SignalMap> = { [K in keyof S]: SignalSchema<S[K]> };

/** Validate a signal payload against its declared schema. Throws with the collected issues. */
export const validateSignal = async <T>(
  schema: SignalSchema<T>,
  name: string,
  payload: unknown,
): Promise<T> => {
  const r = await schema["~standard"].validate(payload);
  if (r.issues) {
    throw new Error(
      `signal "${name}" payload failed validation: ${r.issues.map((i) => i.message).join("; ")}`,
    );
  }
  return r.value;
};

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
   * `ctx.signal` / `engine.signal` and is never read at runtime. Build it with {@link signalType}.
   */
  signals?: SignalSchemas<S>;
  /** Per-flow overrides of the engine's operational policy — e.g. a critical flow that must `"fail"` on drift. */
  policy?: FlowPolicy;
}

/** Per-flow overrides of the engine's operational policy, merged over the engine defaults. */
export interface FlowPolicy {
  drift?: DriftPolicy;
  maxFanOut?: number;
  maxDepth?: number;
}

/** Validate `input` against a flow's (or contract's) schema (if any). Throws with the collected issues. */
export const validateInput = async <I>(
  flow: { name: string; input?: InputSchema<I> },
  input: I,
): Promise<I> => {
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

/**
 * A flow's submit-side contract — its identity (`name`/`version`) plus typed input, output, and
 * signals, WITHOUT the run body. A caller that doesn't own the implementation (another service, or a
 * Go worker sharing the database) can `submit`/`result`/`signal` against it with full type-safety;
 * `submit` accepts a {@link Flow} or a `Contract` interchangeably. The declaring worker still owns
 * execution and the authoritative input validation.
 */
export interface Contract<I = unknown, O = unknown, S extends SignalMap = NoSignals> {
  name: string;
  version: number;
  /** Optional submit-side input validator — the executing worker validates authoritatively regardless. */
  input?: InputSchema<I>;
  signals?: SignalSchemas<S>;
  /** Phantom output type — carried for `submit`→`result` typing; never present at runtime. */
  readonly __out?: O;
}

/**
 * Declare a flow's {@link Contract} for cross-service typed submits. The output type is explicit
 * (there is no body to infer it from): `defineContract<Input, Output, Signals>({ name, version })`.
 */
export const defineContract = <I = unknown, O = unknown, S extends SignalMap = NoSignals>(
  contract: Contract<I, O, S>,
): Contract<I, O, S> => contract;

/** A flow of any shape — the registry and executor dispatch flows type-erased. */
export type AnyFlow = Flow<any, any, any>;

/** One child of a fan-out `ctx.invoke([...])`: a flow and its input. */
export interface InvokeSpec<CI = any, CO = any> {
  flow: Flow<CI, CO, any>;
  input: CI;
}

/**
 * The `{ flow, input }` shape a fan-out spec must have, with `input` bound to `F`'s OWN input type.
 * Mapping it over an inferred flow tuple is what lets `ctx.invoke([{ flow: a, input }, ...])`
 * type-check each input against its own flow instead of accepting `any`.
 */
export type InvokeSpecFor<F> = F extends Flow<infer CI, any, any> ? { flow: F; input: CI } : never;

/** The tuple of child outputs a fan-out over flows `F` resolves to — each flow's output, in order. */
export type FlowOutputs<F extends readonly AnyFlow[]> = {
  readonly [K in keyof F]: F[K] extends Flow<any, infer CO, any> ? CO : never;
};

/** A registry the executor resolves a run's `(name, version)` against to its {@link Flow}. */
export type FlowRegistry = ReadonlyMap<string, AnyFlow>;

/** @internal */
export const flowKey = (name: string, version: number): string => `${name}@${version}`;

/** Build a {@link FlowRegistry} from a list of flows. */
export const registry = (flows: readonly AnyFlow[]): FlowRegistry =>
  new Map(flows.map((f) => [flowKey(f.name, f.version), f]));
