---
"iterativeflow": minor
---

Enqueue-only handles from a flow contract.

A `FlowContract` is the light, body-free identity of a flow — `name`, `version`,
and input schema. An API process that only starts flows can now import the
contract (not the flow body) and build a typed `.start` handle from it, so it
never pulls the body's heavy transitive deps (native addons, etc.) into its
image.

Additive — nothing existing changes:

- `defineContract<I, O>({ name, version, input })` → `FlowContract<I, O>`.
- `flow(contract)` builder overload — seeds `name`/`version`/`input` and
  constrains `.output(...)` to the contract's output type, so the worker's body
  cannot drift from the enqueue-only callers.
- `engine.enqueueHandle(contract)` → a typed `FlowHandle` that enqueues but
  registers no body (adds nothing to the worker's task list, per ADR 0001).
- `engine.enqueue(name, version, input, opts?)` — untyped escape hatch for
  dynamic/codegen callers.

Composes with per-flow routing: the enqueued run lands under
`flow:run:<name>@<version>` and only the worker that registered the body claims
it. `ctx.invoke` already accepts an enqueue-only handle (it reads only
name/version/input), so a parent can invoke a child it doesn't implement.
