import type { StandardSchemaV1 } from "../util/standard-schema";

/**
 * The light, body-free identity of a flow: `name`, `version`, and input schema,
 * plus a phantom output type `O`. An enqueue-only process imports the contract
 * (not the flow body) to obtain a typed `.start` handle via
 * {@link Engine.enqueueHandle}, so it never pulls the body's heavy transitive
 * deps into its image. The worker builds the real flow *from* the same contract
 * (`flow(contract)`), so name/version/input/output cannot drift between them.
 *
 * `O` is carried only by the phantom `__output` marker — never set at runtime.
 * It threads the output type the implementation must satisfy through to
 * `FlowHandle<I, O>` and constrains `flow(contract).output(...)`.
 */
export interface FlowContract<I, O> {
  /** Flow name — the routing key (`flow:run:<name>@<version>`) and `runs.name`. */
  readonly name: string;
  /** Schema version. Must match the implementing flow's version. */
  readonly version: number;
  /** Standard Schema validator for the run input. */
  readonly input?: StandardSchemaV1<unknown, I>;
  /** Phantom carrier for the output type `O`. Never present at runtime. */
  readonly __output?: O;
}

/**
 * Define a {@link FlowContract}. Pass the output type as the second type
 * argument so the implementation and `.start()` callers stay typed:
 *
 * @example
 * ```ts
 * const cloneContract = defineContract<{ mediaId: string }, { status: "done" }>({
 *   name: "clone-media",
 *   version: 1,
 *   input: z.object({ mediaId: z.string() }),
 * });
 * ```
 */
export const defineContract = <I, O = unknown>(contract: {
  name: string;
  version: number;
  input?: StandardSchemaV1<unknown, I>;
}): FlowContract<I, O> => contract;
