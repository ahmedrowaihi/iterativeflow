# Typed flows & signals

v2 flows are typesafe end to end: the input type, the **output** type, and each **signal's** payload
type all flow through `submit` / `result` / `signal` with no casts. This restores the per-flow
type-safety v1 had via `FlowHandle<I, O>` / `FlowContract<I, O>`, and adds typed signals on top (v1
signals were `unknown` on both ends).

## Output type — recovered at `result`

`submit` returns a `RunHandle<O>` — a run id branded with the flow's output type. Pass it to `result`
and the output comes back typed. (It is a `string` at runtime, so it works anywhere a `runId` does.)

```ts
const order = defineFlow({
  name: "order",
  version: 1,
  run: async (ctx, input: { orderId: string }) => ({
    total: 42,
    currency: "USD",
  }),
});

const handle = await engine.submit(order, { orderId: "o1" }); // RunHandle<{ total; currency }>
const r = await engine.result(handle);
r.output?.total; // number ✅  (was `unknown` — you had to cast)
```

## Signals — typed on both ends

Declare a flow's signals once with `type<T>()`. It is type-only (no runtime cost) and drives both the
**await** side (`ctx.signal`) and the **send** side (`engine.signal`).

```ts
import { defineFlow, type } from "@iterativeflow/core";

const approval = defineFlow({
  name: "approval",
  version: 1,
  signals: { approve: type<{ by: string }>() },
  run: async (ctx, input: { orderId: string }) => {
    const decision = await ctx.signal("approve"); // inferred: { by: string } ✅ no cast
    return { orderId: input.orderId, approvedBy: decision.by };
  },
});

const h = await engine.submit(approval, { orderId: "o1" });

await engine.signal(h, "approve", { by: "reviewer" }); // ✅
await engine.signal(h, "aprove", { by: "reviewer" }); // ❌ compile error: unknown signal name
await engine.signal(h, "approve", { user: "reviewer" }); // ❌ compile error: wrong payload shape
```

Without a contract these are the two classic runtime bugs — a typo'd signal name (delivered nowhere,
run hangs forever) and a mismatched payload (the flow reads `undefined`). Both compile today; with the
`signals` map they are caught at build time.

### Signals are Standard-Schema, exactly like `input`

A signal entry is any Standard-Schema validator — the same zod / valibot / arktype schema you'd use
for a flow's `input`. When you give one, the payload is **validated (and parsed) as the flow consumes
it**, and a bad payload fails the run with the validator's message — the same contract as `input` at
submit. `type<T>()` is the escape hatch: a type-only identity validator for when you want the type
without the runtime check.

```ts
import { z } from "zod";

const approval = defineFlow({
  name: "approval",
  version: 1,
  signals: {
    approve: z.object({ by: z.string() }), // validated at consume — a bad payload fails the run
    cancel: type<{ reason: string }>(), // type-only, no runtime validation
  },
  run: async (ctx) => {
    const { by } = await ctx.signal("approve"); // { by: string }, already validated
    // ...
  },
});
```

## Strictness & compatibility

On the **await** side, a flow that declares a `signals` map may only `ctx.signal(<declared name>)` —
an undeclared name is a compile error, symmetric with the send side. A flow with **no** `signals`
map keeps taking any name and returns `unknown` (cast the result, or declare the signal).

One deliberate break from the earliest alpha: the old call-site form `ctx.signal<Payload>(name)` no
longer types the payload — the type parameter is now the signal _name_. That form was an unchecked
assertion anyway; the `signals` map replaces it with a real, both-ends-checked declaration. Migrate
`ctx.signal<Foo>("x")` to either a `signals` entry or `(await ctx.signal("x")) as Foo`.

`RunHandle` is a `string`, so code that stored a `runId`, or called `result(runId)` /
`signal(runId, name, payload)` with a plain string, keeps working untyped.

The memory package's `contract.test.ts` pins all of this — including the `@ts-expect-error` cases on
both the send and await sides, which fail the build if the strictness ever regresses.
