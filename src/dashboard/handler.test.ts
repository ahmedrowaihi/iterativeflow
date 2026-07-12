import { randomUUID } from "node:crypto";
import { PGlite } from "@electric-sql/pglite";
import { drizzle } from "drizzle-orm/pglite";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { z } from "zod";
import { createEngine } from "../engine/engine";
import type { Engine } from "../engine/engine";
import type { CronSpec } from "../engine/types";
import { silentLogger } from "../engine/test-helpers";
import type { WorkflowDb } from "../storage/db";
import { applyFlowSchema } from "../storage/setup";
import { flow } from "../builder/flow";
import { FlowRuntimeError } from "../util/errors";
import { createOutboxEnqueue, createWakeOutboxTable } from "../adapters/serverless/outbox";
import { createServerlessDispatcher } from "../adapters/serverless/dispatcher";
import { drainAndRun } from "../adapters/serverless/runner";
import { createFlowsDashboard, type FlowsDashboard } from "./handler";

// The dashboard consumes only the public Engine API, so a serverless-style
// engine on PGlite (no real pg Pool, no resident worker) exercises it fully.
// health() pings the pool directly, so the fake answers SELECT 1.
const fakePool = {
  options: { max: 5 },
  query: async () => ({ rows: [{ "?column?": 1 }] }),
} as never;

const BASE = "http://dashboard.test/admin/flows";

const get = (dash: FlowsDashboard, path: string) => dash.fetch(new Request(`${BASE}${path}`));

const post = (dash: FlowsDashboard, path: string, init?: RequestInit) =>
  dash.fetch(
    new Request(`${BASE}${path}`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: "{}",
      ...init,
    }),
  );

// Bodies are asserted structurally per test; `any` keeps the assertions terse.
// oxlint-disable-next-line typescript/no-explicit-any
const readJson = async (res: Response): Promise<any> => res.json();

const getJson = async (dash: FlowsDashboard, path: string) => readJson(await get(dash, path));

describe("flows dashboard handler", () => {
  let client: PGlite;
  let db: WorkflowDb;
  let engine: Engine;
  let dash: FlowsDashboard;

  beforeEach(async () => {
    client = new PGlite();
    await client.waitReady;
    db = drizzle({ client }) as unknown as WorkflowDb;
    await applyFlowSchema(db);
    await createWakeOutboxTable(db);

    engine = createEngine({
      db,
      pool: fakePool,
      logger: silentLogger,
      worker: { enqueue: createOutboxEnqueue() },
      dispatcher: createServerlessDispatcher(),
      results: "poll",
      reconciler: { graceMs: 0, runningStuckMs: 0 },
    });
    engine.register(
      flow("greet")
        .step("say", ({ input }) => ({ said: input }))
        .output(({ input }) => input)
        .build(),
    );
    engine.register(
      flow("boom")
        .step("explode", () => {
          throw new FlowRuntimeError({
            code: "KABOOM",
            message: "it broke",
            nonRetryable: true,
          });
        })
        .build(),
    );
    engine.register(
      flow("nap")
        .step("first", () => "done")
        .sleep("1h")
        .step("second", () => "after nap")
        .build(),
    );
    engine.register(
      flow("approval")
        .step("queue", () => "queued")
        .signal("approve", { schema: z.object({ approverId: z.string(), approved: z.boolean() }) })
        .build(),
    );

    dash = createFlowsDashboard({ engine });
  });

  afterEach(async () => {
    await client.close();
  });

  const tick = async () => (await drainAndRun(engine, db)).ran;

  const startDone = async (input: unknown = { ok: true }, tags?: string[]) => {
    const { runId } = await engine.enqueue("greet", 1, input, tags ? { tags } : undefined);
    await tick();
    return runId;
  };

  const startFailed = async () => {
    const { runId } = await engine.enqueue("boom", 1, {});
    await tick();
    return runId;
  };

  const startSleeping = async () => {
    const { runId } = await engine.enqueue("nap", 1, {});
    await tick();
    return runId;
  };

  const startAwaitingSignal = async () => {
    const { runId } = await engine.enqueue("approval", 1, {});
    await tick();
    return runId;
  };

  it("serves the app on GET with a <base> pinned to the mount path", async () => {
    const res = await get(dash, "");
    expect(res.status).toBe(200);
    expect(res.headers.get("content-type")).toContain("text/html");
    const body = await res.text();
    expect(body).toContain('<base href="/admin/flows/">');
  });

  it("keeps an existing trailing slash in the <base>", async () => {
    const body = await (await get(dash, "/")).text();
    expect(body).toContain('<base href="/admin/flows/">');
  });

  it("serves the app for unknown GET paths so deep mounts keep working", async () => {
    const res = await get(dash, "/some/other/page");
    expect(res.status).toBe(200);
    expect(res.headers.get("content-type")).toContain("text/html");
  });

  it("rejects non-GET requests outside the API", async () => {
    const res = await dash.fetch(new Request(BASE, { method: "PUT" }));
    expect(res.status).toBe(405);
  });

  it("serves the app (not 404) when the mount path itself contains /api/", async () => {
    const res = await dash.fetch(new Request("http://dashboard.test/api/flows/"));
    expect(res.status).toBe(200);
    expect(res.headers.get("content-type")).toContain("text/html");
  });

  it("still routes the dashboard's own /api/ under an /api/-containing mount", async () => {
    const res = await dash.fetch(new Request("http://dashboard.test/api/flows/api/health"));
    expect(res.status).toBe(200);
  });

  it("generates :root/.dark token blocks from the typed theme, after the built-in stylesheet", async () => {
    const themed = createFlowsDashboard({
      engine,
      theme: {
        light: { primary: "hotpink", "status-done": "#0f0" },
        dark: { primary: "deeppink" },
        css: ".badge { letter-spacing: .02em; }",
      },
    });
    const body = await (await themed.fetch(new Request(BASE))).text();
    expect(body).toContain(
      '<style id="iflow-theme">:root { --primary: hotpink; --status-done: #0f0; }',
    );
    expect(body).toContain(".dark { --primary: deeppink; }");
    expect(body).toContain(".badge { letter-spacing: .02em; }");
    // Override lives in <head>, before the app mount point in <body>.
    expect(body.indexOf('id="iflow-theme"')).toBeGreaterThan(-1);
    expect(body.indexOf('id="iflow-theme"')).toBeLessThan(body.indexOf('id="app"'));
  });

  it("omits the theme tag when no theme is given", async () => {
    const body = await (await get(dash, "")).text();
    expect(body).not.toContain('id="iflow-theme"');
  });

  it("lists runs without heavy fields and with an error summary", async () => {
    await startDone({ big: "x".repeat(50) });
    await startFailed();

    const res = await get(dash, "/api/runs");
    expect(res.status).toBe(200);
    const body = await readJson(res);
    expect(body.runs).toHaveLength(2);
    expect(body.next).toBeNull();

    const failed = body.runs.find((r: { name: string }) => r.name === "boom");
    expect(failed.status).toBe("failed");
    expect(failed.error).toEqual({ code: "KABOOM", message: "it broke" });
    expect(failed).not.toHaveProperty("input");
    expect(failed).not.toHaveProperty("output");
  });

  it("filters by status, name, and tag", async () => {
    await startDone({}, ["alpha"]);
    await startFailed();

    const byStatus = await getJson(dash, "/api/runs?status=failed");
    expect(byStatus.runs).toHaveLength(1);
    expect(byStatus.runs[0].name).toBe("boom");

    const byName = await getJson(dash, "/api/runs?name=greet");
    expect(byName.runs).toHaveLength(1);

    const byTag = await getJson(dash, "/api/runs?tag=alpha");
    expect(byTag.runs).toHaveLength(1);
    expect(byTag.runs[0].tags).toEqual(["alpha"]);
  });

  it("paginates with the keyset cursor", async () => {
    for (let i = 0; i < 3; i++) await startDone({ i });

    const first = await getJson(dash, "/api/runs?limit=2");
    expect(first.runs).toHaveLength(2);
    expect(first.next).not.toBeNull();

    const cursor = `cursorCreatedAt=${encodeURIComponent(first.next.createdAt)}&cursorId=${first.next.id}`;
    const second = await getJson(dash, `/api/runs?limit=2&${cursor}`);
    expect(second.runs).toHaveLength(1);
    expect(second.next).toBeNull();

    const ids = new Set([...first.runs, ...second.runs].map((r: { id: string }) => r.id));
    expect(ids.size).toBe(3);
  });

  it("rejects bad query params", async () => {
    expect((await get(dash, "/api/runs?status=exploded")).status).toBe(400);
    expect((await get(dash, "/api/runs?limit=0")).status).toBe(400);
    expect((await get(dash, "/api/runs?limit=501")).status).toBe(400);
    expect((await get(dash, "/api/runs?since=not-a-date")).status).toBe(400);
    expect((await get(dash, "/api/runs?cursorId=abc")).status).toBe(400);
  });

  it("returns run detail with capped JSON payloads", async () => {
    const runId = await startDone({ hello: "world" });

    const res = await get(dash, `/api/runs/${runId}`);
    expect(res.status).toBe(200);
    const body = await readJson(res);
    expect(body.run.id).toBe(runId);
    expect(body.run.status).toBe("done");
    expect(body.run.input.truncated).toBe(false);
    expect(JSON.parse(body.run.input.preview)).toEqual({ hello: "world" });
    expect(body.steps).toHaveLength(1);
    expect(body.steps[0].status).toBe("ok");
    expect(body.timers).toEqual([]);
    expect(body.signals).toEqual([]);
  });

  it("truncates payloads past jsonCap and reports the full size", async () => {
    const small = createFlowsDashboard({ engine, jsonCap: 20 });
    const runId = await startDone({ blob: "y".repeat(500) });

    const body = await getJson(small, `/api/runs/${runId}`);
    expect(body.run.input.truncated).toBe(true);
    expect(body.run.input.preview).toHaveLength(20);
    expect(body.run.input.size).toBeGreaterThan(500);
  });

  it("shows sleeps for a suspended run and the full error for a failed one", async () => {
    const sleeping = await startSleeping();
    const napBody = await getJson(dash, `/api/runs/${sleeping}`);
    expect(napBody.run.status).toBe("sleeping");
    expect(napBody.timers).toHaveLength(1);
    expect(napBody.timers[0].firedAt).toBeNull();

    const failed = await startFailed();
    const boomBody = await getJson(dash, `/api/runs/${failed}`);
    expect(boomBody.run.error.code).toBe("KABOOM");
  });

  it("404s an unknown run", async () => {
    expect((await get(dash, `/api/runs/${randomUUID()}`)).status).toBe(404);
  });

  it("cancels an active run", async () => {
    const runId = await startSleeping();

    const res = await post(dash, `/api/runs/${runId}/cancel`, {
      body: JSON.stringify({ reason: "operator said so" }),
    });
    expect(res.status).toBe(200);
    expect((await engine.status(runId))?.run.status).toBe("canceled");
  });

  it("retries a failed run and maps non-failed/missing to 409/404", async () => {
    const failed = await startFailed();
    const res = await post(dash, `/api/runs/${failed}/retry`);
    expect(res.status).toBe(200);
    expect((await readJson(res)).kind).toBe("queued");
    expect((await engine.status(failed))?.run.status).toBe("pending");

    const done = await startDone();
    const conflict = await post(dash, `/api/runs/${done}/retry`);
    expect(conflict.status).toBe(409);
    expect((await readJson(conflict)).kind).toBe("not_failed");

    expect((await post(dash, `/api/runs/${randomUUID()}/retry`)).status).toBe(404);
  });

  it("delivers a signal to a run awaiting one", async () => {
    const runId = await startAwaitingSignal();
    expect((await engine.status(runId))?.run.status).toBe("awaiting_signal");

    const res = await post(dash, `/api/runs/${runId}/signal`, {
      body: JSON.stringify({ name: "approve", payload: { approverId: "u1", approved: true } }),
    });
    expect(res.status).toBe(200);
    expect((await readJson(res)).kind).toBe("delivered");
  });

  it("422s a signal payload that fails the declared schema", async () => {
    const runId = await startAwaitingSignal();

    const res = await post(dash, `/api/runs/${runId}/signal`, {
      body: JSON.stringify({ name: "approve", payload: { approverId: 42 } }),
    });
    expect(res.status).toBe(422);
    expect((await readJson(res)).kind).toBe("invalid_payload");
    expect((await engine.status(runId))?.run.status).toBe("awaiting_signal");
  });

  it("400s a signal without a name", async () => {
    const runId = await startAwaitingSignal();
    const res = await post(dash, `/api/runs/${runId}/signal`, {
      body: JSON.stringify({ payload: { approverId: "u1" } }),
    });
    expect(res.status).toBe(400);
  });

  it("rejects mutations without a JSON content type", async () => {
    const runId = await startSleeping();
    const res = await post(dash, `/api/runs/${runId}/cancel`, {
      headers: { "content-type": "text/plain" },
    });
    expect(res.status).toBe(415);
    expect((await engine.status(runId))?.run.status).toBe("sleeping");
  });

  it("405s wrong methods on API routes", async () => {
    expect((await post(dash, "/api/runs")).status).toBe(405);
    expect((await post(dash, "/api/health")).status).toBe(405);
    const del = await dash.fetch(
      new Request(`${BASE}/api/runs/${randomUUID()}`, { method: "DELETE" }),
    );
    expect(del.status).toBe(405);
  });

  const testCrons: CronSpec[] = [
    { name: "sweep", schedule: "*/5 * * * *", run: () => ({ swept: 3 }) },
    {
      name: "boom-cron",
      schedule: "0 * * * *",
      run: () => {
        throw new Error("cron exploded");
      },
    },
  ];

  it("returns an empty list when no crons are configured", async () => {
    expect(await getJson(dash, "/api/crons")).toEqual({ crons: [] });
  });

  it("lists configured crons with defaults filled in, and never ships run", async () => {
    const withCrons = createFlowsDashboard({ engine, crons: testCrons });
    const body = await getJson(withCrons, "/api/crons");
    expect(body.crons).toEqual([
      {
        name: "sweep",
        schedule: "*/5 * * * *",
        timezone: "UTC",
        overlap: "skip",
        jitterMs: 0,
        backfillPeriod: 0,
      },
      {
        name: "boom-cron",
        schedule: "0 * * * *",
        timezone: "UTC",
        overlap: "skip",
        jitterMs: 0,
        backfillPeriod: 0,
      },
    ]);
    for (const c of body.crons) expect(c).not.toHaveProperty("run");
  });

  it("triggers a cron and returns its capped result", async () => {
    const withCrons = createFlowsDashboard({ engine, crons: testCrons });
    const res = await post(withCrons, "/api/crons/sweep/run");
    expect(res.status).toBe(200);
    const body = await readJson(res);
    expect(body.ok).toBe(true);
    expect(body.result.truncated).toBe(false);
    expect(JSON.parse(body.result.preview)).toEqual({ swept: 3 });
  });

  it("404s an unknown cron name", async () => {
    const withCrons = createFlowsDashboard({ engine, crons: testCrons });
    expect((await post(withCrons, "/api/crons/nope/run")).status).toBe(404);
  });

  it("surfaces a thrown error from the cron body as a 500", async () => {
    const withCrons = createFlowsDashboard({ engine, crons: testCrons });
    const res = await post(withCrons, "/api/crons/boom-cron/run");
    expect(res.status).toBe(500);
    expect((await readJson(res)).error).toBe("cron exploded");
  });

  it("rejects a trigger without a JSON content type", async () => {
    const withCrons = createFlowsDashboard({ engine, crons: testCrons });
    const res = await post(withCrons, "/api/crons/sweep/run", {
      headers: { "content-type": "text/plain" },
    });
    expect(res.status).toBe(415);
  });

  it("405s wrong methods on crons routes", async () => {
    const withCrons = createFlowsDashboard({ engine, crons: testCrons });
    expect((await post(withCrons, "/api/crons")).status).toBe(405);
    const wrongMethod = await withCrons.fetch(
      new Request(`${BASE}/api/crons/sweep/run`, { method: "GET" }),
    );
    expect(wrongMethod.status).toBe(405);
  });

  it("passes health through", async () => {
    const res = await get(dash, "/api/health");
    expect(res.status).toBe(200);
    const body = await readJson(res);
    expect(body.db).toBe(true);
    // This process never called listen(); health is per-process.
    expect(body.worker).toBe(false);
  });
});
