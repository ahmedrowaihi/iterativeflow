# ADR 0002 — Enqueue-only handles from a flow contract

- **Status:** Accepted
- **Date:** 2026-06-22
- **Deciders:** iterativeflow maintainers
- **Relates to:** [ADR 0001 — per-flow task routing](./0001-per-flow-task-routing.md)

## Context

A flow definition couples two very different things:

```ts
interface FlowDefinition<I, O> {
  readonly name: string;
  readonly version: number;
  readonly input?: StandardSchemaV1<unknown, I>;   // light — the contract
  readonly nodes: ReadonlyArray<FlowNode>;         // heavy — step bodies
  readonly body: (ctx, input) => Promise<O>;       // heavy — node-av etc.
  readonly signalSchemas?: ...;
}
```

`engine.register(def)` does two independent jobs with it:

1. **Execute** — `registry.register({ name, version, run: body, ... })` so this
   process can run the flow (and, post-ADR-0001, adds `flow:run:<name>@<v>` to
   the worker's task list).
2. **Enqueue** — `buildHandle(name, version, inputSchema)` returns the
   `FlowHandle` whose `.start()` validates input and inserts+enqueues a run.

Crucially, **the handle never touches `body` or `nodes`** —
`createHandleFactory` (`src/engine/handle.ts`) takes only
`(name, version, inputSchema)`. Enqueue and execute are already separable in the
code; today they are only coupled because `register` is the single door to both
and it demands a full `FlowDefinition`.

That coupling forces an **enqueue-only caller to import the body it will never
run.** Concretely, a downstream HTTP/API process that starts flows on request
(but does not `listen()`) must `engine.register(cloneFlow)` purely to obtain
`cloneFlow`'s `.start` handle — dragging the flow's heavy transitive deps
(`node-av`, a native libav addon) into an image that never executes a step. The
goal: let such a process get a **typed** `.start` handle without importing the
body, and without the name/input drifting out of sync with the real flow.

## Decision

Introduce a **flow contract** — the light `{ name, version, input }` identity of
a flow, with no body — and an engine method that builds an **enqueue-only
handle** from it.

```ts
// the typed identity, body-free
interface FlowContract<I, O> {
  readonly name: string;
  readonly version: number;
  readonly input?: StandardSchemaV1<unknown, I>;
  // O is a phantom type param: the output type the implementation must satisfy.
}

const defineContract = <I, O = unknown>(c: {
  name: string;
  version: number;
  input?: StandardSchemaV1<unknown, I>;
}): FlowContract<I, O> => c;

// on the Engine:
enqueueHandle<I, O>(contract: FlowContract<I, O>): FlowHandle<I, O>;
```

- **`enqueueHandle(contract)`** calls the existing
  `createHandleFactory(...)(contract.name, contract.version, contract.input)`
  and returns the handle **without** registering a body. It does **not** add the
  registry entry and does **not** add a task to this process's task list — so the
  process enqueues the flow but never claims it (exactly the ADR-0001 routing
  contract: you only execute what you registered a body for).
- `.start(input)` validates `input` against `contract.input` (fail-fast at the
  caller, before the run row is written) and enqueues under
  `flow:run:${name}@${version}` (ADR 0001). `.result()` / `.output()` work as
  today via the `flow_terminal` LISTEN channel, typed to `O`.

### Drift is prevented by implementing _against_ the contract

The contract is the single source of `name` + `version` + input schema + output
type. The body is built **from** the contract, so the two cannot drift:

```ts
// clone.contract.ts  — LIGHT. imports only the schema lib + iterativeflow.
export const cloneContract = defineContract<{ mediaId: string }, { status: "done" | "disabled" }>({
  name: "clone-media",
  version: 1,
  input: z.object({ mediaId: z.string() }),
});

// clone.flow.ts  — HEAVY (node-av). lives with the worker.
export const cloneFlow = flow(cloneContract)              // seeded by the contract
  .step("copy", async ({ input }) => copyWithNodeAv(input.mediaId))  // input: { mediaId: string }
  .output(({ ... }) => ({ status: "done" as const }))     // must be assignable to O
  .build();
```

- **API process** imports `cloneContract` only → `engine.enqueueHandle(cloneContract).start({ mediaId })`
  is fully typed, no `node-av`.
- **Worker process** imports `cloneFlow` → `engine.register(cloneFlow)` → executes.
- Both agree on `clone-media@1` and its input shape **because they share the
  contract object**. Rename the flow, bump the version, or change the input
  schema in one place; a mismatched `.start` call or a body that returns the
  wrong `O` is a compile error, not a runtime `No flow registered` / bad-payload
  surprise.

`flow(contract)` (builder seeded by a contract) is the proposed ergonomic. The
builder pre-fills `name`/`version`/`input` from the contract and constrains
`.output()`'s return to the contract's `O`. The existing `flow(name: string)`
overload stays for flows with no separate contract.

### Low-level escape hatch

`enqueueHandle` is sugar over a primitive that takes the identity directly, for
dynamic/codegen callers that don't have a static contract object:

```ts
enqueue(name: string, version: number, input: unknown, opts?: StartOpts): Promise<{ runId: string }>;
```

Untyped by nature (`input: unknown`); `enqueueHandle` is the typed front door and
should be the default.

## Why not the alternatives

- **Symbols for flow identity.** The flow name is **serialized** — it is the
  graphile `task_identifier` (`flow:run:<name>@<v>`) and the `runs.name` column.
  A symbol has no stable serialized form, so it can't be the routing key; you'd
  still need the string. Symbols add identity uniqueness we don't need and lose
  the serialization we do. The contract already carries the string name _and_ the
  static type — strictly more useful.
- **A hand-maintained `Record<flowName, inputType>` map** consumed by a
  string-keyed `enqueue<K extends keyof M>(name: K, input: M[K])`. The map is a
  second source of truth that drifts from the flow defs; deriving it
  automatically would require importing the defs — i.e. the very `node-av`
  import we're trying to avoid. The contract makes each flow self-describe its
  light identity in one place.
- **Keep coupling; tree-shake the body out.** `node-av` is a native addon with
  side-effectful `require`; it is not reliably tree-shaken, and on Bun an unused
  native addon still loads at import. Decoupling at the type level is the robust
  fix.

## Consequences

- An enqueue-only process imports contracts, not bodies → no `node-av` in its
  image; smaller image, faster cold start, no native-addon load on a path that
  never runs steps.
- Composes cleanly with ADR 0001: `enqueueHandle` registers no body, so it adds
  nothing to the task list — the process enqueues but does not claim. The worker
  that registered the body claims it.
- New public surface: `FlowContract`, `defineContract`, `engine.enqueueHandle`,
  `engine.enqueue`, and a `flow(contract)` builder overload. Run
  `npm run api:update` and commit `etc/iterativeflow.api.md`. Document in the
  changelog (additive → minor bump; nothing existing changes).
- `ctx.invoke(handle, input)` should accept an `enqueueHandle`-produced handle
  too, so a parent flow can invoke a child it doesn't itself implement (same
  decoupling, one level down). Verify `createStartChild` only needs name+version
  +input (it should — child runs are enqueued, then executed by whoever
  registered the body).

## Open questions for the implementer

1. **Builder ergonomics.** `flow(contract)` vs `contract.implement((ctx, input) => …)`
   vs `defineFlow({ contract, body })`. Pick whichever composes best with the
   existing `FlowBuilder` generics; the requirement is only that `input` is typed
   `I` and the output is constrained to `O` so the body can't drift from the
   contract.
2. **Method name.** `enqueueHandle` vs `handleFor` vs `proxy` vs `client`. It
   returns a real `FlowHandle` that can `.start`/`.result`/`.output` but never
   executes locally — name it for that.
3. **Output typing.** This ADR carries `O` as a phantom type param enforced by
   `flow(contract)`. Consider whether an optional **output schema** on the
   contract is worth it (runtime-validated results, typed `.output()` without
   relying on the builder seam). Likely a follow-up, not v1.
4. **Validation source.** `enqueueHandle` validates against `contract.input`. The
   worker's body, built from the same contract, shares that schema — confirm
   there is exactly one schema object in play, no re-declaration.

## Resolved (as implemented)

1. **Builder ergonomics** — `flow(contract)` overload. The existing
   `flow(name)` stays. A third builder type param `Out` (default `unknown`)
   carries the contract's output bound; `.output<O2 extends Out>(...)` infers
   freely for `flow(name)` (bound is `unknown`) and constrains to the contract's
   `O` for `flow(contract)`.
2. **Method name** — `enqueueHandle`. The low-level hatch is `engine.enqueue`.
3. **Output typing** — phantom `O` carried by an optional `__output?: O` marker
   on `FlowContract` (never set at runtime), threaded into `FlowHandle<I, O>`.
   No output schema in v1 (left as a follow-up).
4. **Validation source** — one schema object. `enqueueHandle` validates against
   `contract.input`; `flow(contract)` seeds the builder's `input` with the _same_
   object — asserted in `contract.test.ts` (`def.input === cloneContract.input`).
5. **`ctx.invoke`** — no change needed. `createStartChild` reads only
   `handle.name`/`handle.version`/`input`, so an `enqueueHandle`-produced handle
   already works as an invoke target.

## Test plan

- **Typed, drift-proof:** a contract + an implementation built from it; assert
  `enqueueHandle(contract).start(wrongShape)` is a _type error_ (e.g. an
  `expect-type` / `tsd`-style check) and `start(rightShape)` compiles.
- **Body-free enqueue executes elsewhere (the lean-API shape):** engine A calls
  `enqueueHandle(contract).start(input)` and never registers the body / never
  `listen()`s; engine B `register`s the full flow + `listen()`s; assert the run
  completes on B. Reuse the two-pool testcontainer harness from
  `multi-instance.test.ts`. Assert A's task list contains no `flow:run:<name>@<v>`
  entry (it registered no body).
- **Routing still holds:** the enqueued job lands under `flow:run:<name>@<v>` and
  only a body-registered worker claims it (ADR 0001 invariant).
- **Escape hatch:** `engine.enqueue(name, version, input)` enqueues an equivalent
  run; a registered worker runs it.

## Tooling gates

```
npm run typecheck && npm run lint && npm run test
npm run api:update     # commit etc/iterativeflow.api.md
npm run docs:check && npm run size:check
```
