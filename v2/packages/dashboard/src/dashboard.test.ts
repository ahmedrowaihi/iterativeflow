import { createEngine, defineFlow } from "@iterativeflow/core";
import { createDashboard } from "#index";
import { createMemoryBackend } from "@iterativeflow/memory";
import { describe, expect, it } from "vitest";

const TERMINAL = new Set(["done", "failed", "canceled"]);

const buildEngine = () => {
  const flow = defineFlow<{ x: number }, number>({
    name: "double",
    version: 1,
    run: async (ctx, input) => ctx.step("d", () => input.x * 2),
  });
  const engine = createEngine(createMemoryBackend(), [flow], {
    now: () => new Date("2030-01-01T00:00:00Z"),
  });
  return { engine, flow };
};

const drive = async (engine: ReturnType<typeof buildEngine>["engine"], id: string) => {
  for (let i = 0; i < 5; i++) {
    const s = await engine.status(id);
    if (s && TERMINAL.has(s.run.status)) return;
    await engine.tick();
  }
};

describe("dashboard fetch handler", () => {
  it("serves the UI at the base path", async () => {
    const { engine } = buildEngine();
    const handler = createDashboard(engine);
    const res = await handler(new Request("http://x/"));
    expect(res.headers.get("content-type")).toContain("text/html");
    expect(await res.text()).toContain("iterativeflow");
  });

  it("lists runs and returns health over the JSON API", async () => {
    const { engine, flow } = buildEngine();
    const handler = createDashboard(engine);
    const id = await engine.submit(flow, { x: 21 });
    await drive(engine, id);

    const runs = (await (await handler(new Request("http://x/api/runs"))).json()) as {
      runs: { id: string }[];
    };
    expect(runs.runs.map((r) => r.id)).toContain(id);

    const health = (await (await handler(new Request("http://x/api/health"))).json()) as {
      done: number;
    };
    expect(health.done).toBe(1);

    const detail = (await (await handler(new Request(`http://x/api/runs/${id}`))).json()) as {
      run: { output: number };
      steps: { cursorKey: string }[];
    };
    expect(detail.run.output).toBe(42);
    expect(detail.steps.map((s) => s.cursorKey)).toContain("s0");
  });

  it("cancels a run through the API", async () => {
    const { engine, flow } = buildEngine();
    const handler = createDashboard(engine);
    const id = await engine.submit(flow, { x: 1 });
    const res = await handler(new Request(`http://x/api/runs/${id}/cancel`, { method: "POST" }));
    expect(((await res.json()) as { ok: boolean }).ok).toBe(true);
    expect((await engine.status(id))?.run.status).toBe("canceled");
  });

  it("404s an unknown run", async () => {
    const { engine } = buildEngine();
    const handler = createDashboard(engine);
    const res = await handler(new Request("http://x/api/runs/nope"));
    expect(res.status).toBe(404);
  });
});
