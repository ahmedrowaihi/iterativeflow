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
  run: async (ctx, input: { orderId: string }) => ({ total: 42, currency: "USD" }),
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

## Backward compatible

A flow with no `signals` map behaves exactly as before: `ctx.signal<T>(name)` still takes `T` at the
call site. `RunHandle` is a `string`, so existing code that stored a `runId` string, or called
`result(runId)` / `signal(runId, name, payload)` with a plain string, keeps working untyped.

The memory package's `contract.test.ts` pins all of this — including the two `@ts-expect-error` cases,
which fail the build if the sender-side strictness ever regresses.
