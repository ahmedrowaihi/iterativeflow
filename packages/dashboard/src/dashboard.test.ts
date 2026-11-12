import { createEngine, defineFlow } from "@iterativeflow/core";
import { createTestHarness } from "@iterativeflow/core/testing";
import { createDashboard } from "#index";
import { createMemoryBackend } from "@iterativeflow/memory";
import { describe, expect, it } from "vitest";

const double = defineFlow<{ x: number }, number>({
  name: "double",
  version: 1,
  run: async (ctx, input) => ctx.step("d", () => input.x * 2),
});

const buildEngine = () => {
  const harness = createTestHarness(createMemoryBackend(), [double]);
  return { engine: harness.engine, flow: double, harness };
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
    const { engine, flow, harness } = buildEngine();
    const handler = createDashboard(engine);
    const id = await engine.submit(flow, { x: 21 });
    await harness.settle(id);

    const runs = await (await handler(new Request("http://x/api/runs"))).json();
    expect(runs).toMatchObject({ runs: expect.arrayContaining([expect.objectContaining({ id })]) });

    const health = await (await handler(new Request("http://x/api/health"))).json();
    expect(health).toMatchObject({ done: 1 });

    const detail = await (await handler(new Request(`http://x/api/runs/${id}`))).json();
    expect(detail).toMatchObject({
      run: { output: 42 },
      steps: [expect.objectContaining({ status: "ok", result: 42 })],
    });
  });

  it("shows a parent's children, and each child links back to its parent", async () => {
    const child = defineFlow<{ x: number }, number>({
      name: "child",
      version: 1,
      run: async (ctx) => {
        await ctx.signal("never");
        return 1;
      },
    });
    const parent = defineFlow<{ x: number }, number>({
      name: "parent",
      version: 1,
      run: async (ctx, input) => ctx.invoke(child, input),
    });
    const engine = createEngine(createMemoryBackend(), [parent, child]);
    const handler = createDashboard(engine);
    const id = await engine.submit(parent, { x: 1 });
    await engine.tick();
    await engine.tick();
    const [kid] = await engine.children(id);

    const detail = await (await handler(new Request(`http://x/api/runs/${id}`))).json();
    expect(detail).toMatchObject({
      run: { status: "awaiting_child" },
      children: [{ id: kid?.id, name: "child", version: 1, status: "awaiting_signal" }],
    });
    const kidDetail = await (await handler(new Request(`http://x/api/runs/${kid?.id}`))).json();
    expect(kidDetail).toMatchObject({ run: { parentRunId: id } });
  });

  it("cancels a run through the API", async () => {
    const { engine, flow } = buildEngine();
    const handler = createDashboard(engine);
    const id = await engine.submit(flow, { x: 1 });
    const res = await handler(new Request(`http://x/api/runs/${id}/cancel`, { method: "POST" }));
    expect(await res.json()).toEqual({ ok: true });
    expect((await engine.status(id))?.run.status).toBe("canceled");
  });

  it("serves the UI and API under a basePath, with the page calling the mounted API", async () => {
    const { engine } = buildEngine();
    const handler = createDashboard(engine, { basePath: "/admin/flows" });
    const page = await (await handler(new Request("http://x/admin/flows"))).text();
    expect(page).toContain('"/admin/flows"+"/api"');
    expect((await handler(new Request("http://x/admin/flows/api/health"))).status).toBe(200);
    expect((await handler(new Request("http://x/admin/flowsX/api/health"))).status).toBe(404);
    expect((await handler(new Request("http://x/api/health"))).status).toBe(404);
  });

  it("404s an unknown run", async () => {
    const { engine } = buildEngine();
    const handler = createDashboard(engine);
    const res = await handler(new Request("http://x/api/runs/nope"));
    expect(res.status).toBe(404);
  });

  it("clamps a hostile ?limit instead of passing it to the store", async () => {
    const { engine } = buildEngine();
    const seen: number[] = [];
    const spy: typeof engine = {
      ...engine,
      listRuns: (filter, page) => {
        seen.push(page.limit);
        return engine.listRuns(filter, page);
      },
    };
    const app = createDashboard(spy);
    for (const raw of ["-1", "abc", "0", "99999", "25"]) {
      expect((await app(new Request(`http://x/api/runs?limit=${raw}`))).status).toBe(200);
    }
    expect(seen).toEqual([50, 50, 50, 200, 25]);
  });

  it("delivers a signal named in the path, with the body as payload and a header idempotency key", async () => {
    const { engine, flow } = buildEngine();
    const app = createDashboard(engine);
    const handle = await engine.submit(flow, { x: 1 });
    const post = () =>
      app(
        new Request(`http://x/api/runs/${handle}/signals/go`, {
          method: "POST",
          headers: { "idempotency-key": "k" },
          body: JSON.stringify({ a: 1 }),
        }),
      );
    expect(await (await post()).json()).toEqual({ delivered: true });
    expect(await (await post()).json()).toEqual({ delivered: false });
    expect((await engine.status(handle))?.signals).toMatchObject([
      { name: "go", payload: { a: 1 } },
    ]);
  });

  it("delivers a signal with no body as an undefined payload", async () => {
    const { engine, flow } = buildEngine();
    const app = createDashboard(engine);
    const handle = await engine.submit(flow, { x: 1 });
    const res = await app(
      new Request(`http://x/api/runs/${handle}/signals/go`, { method: "POST" }),
    );
    expect(await res.json()).toEqual({ delivered: true });
  });

  it("rejects a non-JSON signal payload", async () => {
    const { engine, flow } = buildEngine();
    const app = createDashboard(engine);
    const handle = await engine.submit(flow, { x: 1 });
    const res = await app(
      new Request(`http://x/api/runs/${handle}/signals/go`, { method: "POST", body: "nope" }),
    );
    expect(res.status).toBe(400);
  });
});
