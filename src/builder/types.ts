import type { FlowContext, SignalOpts, StepArg, StepOpts } from "../engine/types";
import type { Duration } from "../util/duration";
import type { StandardSchemaV1 } from "../util/standard-schema";

/** Node in a builder-produced flow plan: one `ctx.step(...)` call. */
export interface StepNode {
  /** Discriminator. */
  kind: "step";
  /** Step name — also the base of its cursor key. */
  name: string;
  /** Step body (the function passed to `.step(name, fn)`). */
  fn: (arg: StepArg<unknown>) => unknown;
  /** Per-step options (retries, backoff, timeout, classify). */
  opts?: StepOpts;
}

/** Node in a builder-produced flow plan: one `ctx.sleep(...)` call. */
export interface SleepNode {
  /** Discriminator. */
  kind: "sleep";
  /** How long to suspend before resuming. */
  duration: Duration;
}

/** Node in a builder-produced flow plan: one `ctx.signal(...)` await. */
export interface SignalNode {
  /** Discriminator. */
  kind: "signal";
  /** Signal name — `signal:<name>` is the canonical cursor key. */
  name: string;
  /** Per-signal options (schema, timeout). */
  opts?: SignalOpts<unknown>;
  /** Combine the channel value with the delivered payload; defaults to replacing the channel. */
  merge?: (input: unknown, payload: unknown) => unknown;
}

/** Node in a builder-produced flow plan: iterate child nodes until `until(channel)` is `true`. */
export interface LoopNode {
  /** Discriminator. */
  kind: "loop";
  /** Stop condition evaluated against the channel value before each iteration. */
  until: (input: unknown) => boolean;
  /** Body executed each iteration. */
  nodes: ReadonlyArray<FlowNode>;
}

/** Discriminated union of builder-produced flow plan nodes. */
export type FlowNode = StepNode | SleepNode | SignalNode | LoopNode;

/** Result of `flow(...).build()`. The shape `engine.register(...)` accepts. */
export interface FlowDefinition<I, O> {
  /** Flow name. */
  readonly name: string;
  /** Schema version. Bump on body-shape changes. */
  readonly version: number;
  /** Standard Schema validator for the run input. */
  readonly input?: StandardSchemaV1<unknown, I>;
  /** Static node tree (drives drift detection via `checkCompat`). */
  readonly nodes: ReadonlyArray<FlowNode>;
  /** Compiled body — the function the engine actually runs. */
  readonly body: (ctx: FlowContext, input: I) => Promise<O>;
  /**
   * Schemas declared via `.signal(name, { schema })` collected from the static
   * node tree at build time. Lets the engine validate incoming payloads at
   * `engine.signal(...)` delivery time, before they are persisted.
   */
  readonly signalSchemas?: ReadonlyMap<string, StandardSchemaV1<unknown, unknown>>;
}
