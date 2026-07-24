import {
  defineFlow,
  registry,
  result,
  signalRun,
  submit,
  tickOnce,
  type,
} from "@iterativeflow/core";
import { describe, expect, it } from "vitest";
import { createMemoryBackend } from "#index";

// This file is a type test as much as a runtime one: the `@ts-expect-error` lines fail to COMPILE
// if the typed-contract surface regresses, and the `.by` / `output.total` reads fail to compile if
// receiver-side signal typing or output typing is lost. Both are invisible to a runtime-only check.

const approval = defineFlow({
  name: "approval",
  version: 1,
  signals: { approve: type<{ by: string }>() },
  run: async (ctx, input: { orderId: string }) => {
    const decision = await ctx.signal("approve"); // inferred: { by: string }
    return { orderId: input.orderId, approvedBy: decision.by, total: 42 };
  },
});

describe("typed contract", () => {
  it("threads output type through submit → result and payload type through signal", async () => {
    const backend = createMemoryBackend();
    const flows = registry([approval]);
    const opts = { batchMax: 8, leaseMs: 60_000 };

    const handle = await submit(backend, approval, { orderId: "o1" });
    await tickOnce(backend, flows, opts); // runs to the signal, parks

    const delivered = await signalRun(backend, handle, "approve", { by: "reviewer" });
    expect(delivered).toBe(true);
    await tickOnce(backend, flows, opts); // resumes and completes

    const r = await result(backend, handle, { timeoutMs: 1000, now: () => Date.now() });
    expect(r.status).toBe("done");
    // r.output is typed as the flow's return — this reads compile only if that type survived.
    expect(r.output?.total).toBe(42);
    expect(r.output?.approvedBy).toBe("reviewer");
  });

  it("rejects a wrong signal name or payload at compile time", async () => {
    const backend = createMemoryBackend();
    const handle = await submit(backend, approval, { orderId: "o1" });

    await signalRun(backend, handle, "approve", { by: "ok" }); // the only valid form
    // @ts-expect-error unknown signal name
    await signalRun(backend, handle, "aprove", { by: "typo" });
    // @ts-expect-error payload shape does not match the declared signal type
    await signalRun(backend, handle, "approve", { approvedBy: "wrong-key" });
  });
});
