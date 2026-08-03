import { registry } from "#engine/flow";
import { type Backend } from "#ports/outbox";
import { tickOnce } from "#engine/worker";
import { PollTimeoutError } from "#engine/signals";
import { describe, expect, it } from "vitest";

/** A backend whose `claim` never resolves — a black-holed connection awaiting a dead
 *  socket. `timer.dueBatch` resolves empty so the hang is isolated to the claim. */
const hangingBackend = (): Backend =>
  ({
    store: {} as Backend["store"],
    wakeup: {} as Backend["wakeup"],
    timer: {
      dueBatch: async () => [],
    } as unknown as Backend["timer"],
    queue: {
      enqueue: async () => undefined,
      claim: () => new Promise<never>(() => undefined),
    } as unknown as Backend["queue"],
  }) satisfies Backend;

const NO_FLOWS = registry([]);

describe("tickOnce poll deadline", () => {
  it("rejects instead of freezing when the claim hangs", async () => {
    await expect(
      tickOnce(hangingBackend(), NO_FLOWS, {
        batchMax: 8,
        leaseMs: 30_000,
        pollTimeoutMs: 20,
      }),
    ).rejects.toBeInstanceOf(PollTimeoutError);
  });

  it("hangs (never rejects) when the deadline is disabled", async () => {
    const settled = await Promise.race([
      tickOnce(hangingBackend(), NO_FLOWS, {
        batchMax: 8,
        leaseMs: 30_000,
        pollTimeoutMs: 0,
      }).then(() => "resolved" as const),
      new Promise<"still-hung">((resolve) => setTimeout(() => resolve("still-hung"), 40)),
    ]);
    expect(settled).toBe("still-hung");
  });
});
