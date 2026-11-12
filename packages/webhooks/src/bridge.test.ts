import {
  type Backend,
  createEngine,
  defineFlow,
  registry,
  type SignalSchema,
  submit,
  tickOnce,
} from "@iterativeflow/core";
import { createMemoryBackend } from "@iterativeflow/memory";
import { createHmac } from "node:crypto";
import { beforeEach, describe, expect, it } from "vitest";
import { webhookSignalBridge } from "#bridge";
import { github } from "#github";

const SECRET = "hook-secret";

const ghSign = (body: string): string =>
  `sha256=${createHmac("sha256", SECRET).update(body).digest("hex")}`;

interface IssueCommentPayload {
  action?: string;
  number: number;
}

const approvedSchema: SignalSchema<{ by: string }> = {
  "~standard": {
    version: 1,
    vendor: "test",
    validate: (v) =>
      v instanceof Object && "by" in v
        ? { value: { by: String(v.by) } }
        : { issues: [{ message: "by is required" }] },
  },
};

const approvalFlow = defineFlow<{ pr: number }, string, { "qa:approved": { by: string } }>({
  name: "await-approval",
  version: 1,
  signals: { "qa:approved": approvedSchema },
  run: async (ctx) => {
    const payload = await ctx.signal("qa:approved");
    return `approved-by:${payload.by}`;
  },
});

describe("webhookSignalBridge (github preset)", () => {
  let backend: Backend;
  const flows = registry([approvalFlow]);
  const now = (): Date => new Date("2030-01-01T00:00:00Z");
  const tick = () => tickOnce(backend, flows, { batchMax: 16, leaseMs: 600_000, now });

  beforeEach(() => {
    backend = createMemoryBackend();
  });

  const parkOnSignal = async (input: { pr: number }): Promise<string> => {
    const runId = await submit(backend, approvalFlow, input);
    await tick();
    expect((await backend.store.loadRun(runId))?.run.status).toBe("awaiting_signal");
    return runId;
  };

  const webhook = async (payload: IssueCommentPayload, delivery: string) => {
    const body = JSON.stringify(payload);
    return {
      body,
      headers: {
        "X-Hub-Signature-256": ghSign(body),
        "X-GitHub-Event": "issue_comment",
        "X-GitHub-Delivery": delivery,
      },
    };
  };

  it("delivers a webhook as a signal that resumes the parked run", async () => {
    const runId = await parkOnSignal({ pr: 42 });
    const bridge = webhookSignalBridge(createEngine(backend, [approvalFlow]), {
      verify: github(SECRET),
      correlate: (event) => {
        expect(event.payload).toEqual({ action: "created", number: 42 });
        return [{ runId, name: "qa:approved", payload: { by: "alice" } }];
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
    const bridge = webhookSignalBridge(createEngine(backend, [approvalFlow]), {
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
    const bridge = webhookSignalBridge(createEngine(backend, [approvalFlow]), {
      verify: github(SECRET),
      correlate: () => [
        { runId: a, name: "qa:approved", payload: { by: "x" } },
        { runId: b, name: "qa:approved", payload: { by: "x" } },
      ],
    });
    const result = await bridge(await webhook({ number: 7 }, "d-fan"));
    expect(result.deliveries.map((d) => d.delivered)).toEqual([true, true]);
  });

  it("rejects a forged webhook before correlating it", async () => {
    let correlated = 0;
    const bridge = webhookSignalBridge(createEngine(backend, [approvalFlow]), {
      verify: github(SECRET),
      correlate: () => {
        correlated++;
        return [];
      },
    });
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
    expect(correlated).toBe(0);
  });

  it("rejects a delivery over the engine's maxPayloadBytes", async () => {
    const runId = await parkOnSignal({ pr: 9 });
    const engine = createEngine(backend, [approvalFlow], { maxPayloadBytes: 64 });
    const bridge = webhookSignalBridge(engine, {
      verify: github(SECRET),
      correlate: () => [{ runId, name: "qa:approved", payload: { by: "x".repeat(100) } }],
    });
    await expect(bridge(await webhook({ number: 9 }, "d-big"))).rejects.toThrow(/maxPayloadBytes/);
    expect((await backend.store.loadRun(runId))?.signals).toEqual([]);
  });
});
