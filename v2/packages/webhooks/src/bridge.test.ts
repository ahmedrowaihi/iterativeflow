import { type Backend, defineFlow, registry, submit, tickOnce } from "@iterativeflow/core";
import { createMemoryBackend } from "@iterativeflow/memory";
import { beforeEach, describe, expect, it } from "vitest";
import { webhookSignalBridge } from "#bridge";
import { github } from "#github";

const SECRET = "hook-secret";

async function ghSign(body: string): Promise<string> {
  const enc = new TextEncoder();
  const key = await crypto.subtle.importKey(
    "raw",
    enc.encode(SECRET),
    { name: "HMAC", hash: "SHA-256" },
    false,
    ["sign"],
  );
  const mac = await crypto.subtle.sign("HMAC", key, enc.encode(body));
  const hex = [...new Uint8Array(mac)].map((b) => b.toString(16).padStart(2, "0")).join("");
  return `sha256=${hex}`;
}

const approvalFlow = defineFlow<{ pr: number }, string>({
  name: "await-approval",
  version: 1,
  run: async (ctx) => {
    const payload = (await ctx.signal("qa:approved")) as { by: string };
    return `approved-by:${payload.by}`;
  },
});

describe("webhookSignalBridge (github preset)", () => {
  let backend: Backend;
  const flows = registry([approvalFlow]);
  const now = (): Date => new Date("2030-01-01T00:00:00Z");
  const tick = (): Promise<unknown> =>
    tickOnce(backend, flows, { batchMax: 16, leaseMs: 600_000, now });

  beforeEach(() => {
    backend = createMemoryBackend();
  });

  const parkOnSignal = async (input: { pr: number }): Promise<string> => {
    const runId = await submit(backend, approvalFlow, input);
    await tick();
    expect((await backend.store.loadRun(runId))?.run.status).toBe("awaiting_signal");
    return runId;
  };

  const webhook = async (payload: unknown, delivery: string) => {
    const body = JSON.stringify(payload);
    return {
      body,
      headers: {
        "X-Hub-Signature-256": await ghSign(body),
        "X-GitHub-Event": "issue_comment",
        "X-GitHub-Delivery": delivery,
      },
    };
  };

  it("delivers a webhook as a signal that resumes the parked run", async () => {
    const runId = await parkOnSignal({ pr: 42 });
    const bridge = webhookSignalBridge(backend, {
      verify: github(SECRET),
      correlate: (event) => {
        const pr = (event.payload as { number: number }).number;
        return pr === 42 ? [{ runId, name: "qa:approved", payload: { by: "alice" } }] : [];
      },
    });

    const result = await bridge(await webhook({ action: "created", number: 42 }, "d-1"));
    expect(result).toMatchObject({
      type: "issue_comment",
      deliveries: [{ runId, name: "qa:approved", delivered: true }],
    });

    await tick();
    const run = (await backend.store.loadRun(runId))?.run;
    expect(run?.status).toBe("done");
    expect(run?.output).toBe("approved-by:alice");
  });

  it("is idempotent on the delivery id (a provider redelivery lands once)", async () => {
    const runId = await parkOnSignal({ pr: 1 });
    const bridge = webhookSignalBridge(backend, {
      verify: github(SECRET),
      correlate: () => [{ runId, name: "qa:approved", payload: { by: "bob" } }],
    });
    const hook = await webhook({ action: "created", number: 1 }, "same-guid");

    const first = await bridge(hook);
    const second = await bridge(hook);
    expect(first.deliveries[0]?.delivered).toBe(true);
    expect(second.deliveries[0]?.delivered).toBe(false);
  });

  it("fans one webhook out to several parked runs", async () => {
    const a = await parkOnSignal({ pr: 7 });
    const b = await parkOnSignal({ pr: 7 });
    const bridge = webhookSignalBridge(backend, {
      verify: github(SECRET),
      correlate: () => [
        { runId: a, name: "qa:approved", payload: { by: "x" } },
        { runId: b, name: "qa:approved", payload: { by: "x" } },
      ],
    });
    const result = await bridge(await webhook({ number: 7 }, "d-fan"));
    expect(result.deliveries.map((d) => d.delivered)).toEqual([true, true]);
  });

  it("rejects an unsigned/forged webhook before touching the store", async () => {
    const bridge = webhookSignalBridge(backend, { verify: github(SECRET), correlate: () => [] });
    await expect(
      bridge({
        body: JSON.stringify({ number: 1 }),
        headers: {
          "X-Hub-Signature-256": "sha256=deadbeef",
          "X-GitHub-Event": "push",
          "X-GitHub-Delivery": "z",
        },
      }),
    ).rejects.toMatchObject({ code: "invalid_signature" });
  });
});
