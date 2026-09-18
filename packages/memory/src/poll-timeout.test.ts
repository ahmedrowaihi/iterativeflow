import { type Backend, PollTimeoutError, registry, tickOnce } from "@iterativeflow/core";
import { describe, expect, it } from "vitest";
import { createMemoryBackend } from "#index";

// A black-holed connection: `claim` awaits a dead socket and never settles.
const hangingBackend = (): Backend => {
  const real = createMemoryBackend();
  return { ...real, queue: { ...real.queue, claim: () => new Promise(() => undefined) } };
};

const NO_FLOWS = registry([]);

describe("tickOnce poll deadline", () => {
  it("rejects instead of freezing when the claim hangs", async () => {
    await expect(
      tickOnce(hangingBackend(), NO_FLOWS, { batchMax: 8, leaseMs: 30_000, pollTimeoutMs: 20 }),
    ).rejects.toBeInstanceOf(PollTimeoutError);
  });

  it("hangs (never rejects) when the deadline is disabled", async () => {
    const settled = await Promise.race([
      tickOnce(hangingBackend(), NO_FLOWS, { batchMax: 8, leaseMs: 30_000, pollTimeoutMs: 0 }).then(
        () => "resolved" as const,
      ),
      new Promise<"still-hung">((resolve) => setTimeout(() => resolve("still-hung"), 40)),
    ]);
    expect(settled).toBe("still-hung");
  });
});
