import { randomUUID } from "node:crypto";
import { PGlite } from "@electric-sql/pglite";
import { drizzle } from "drizzle-orm/pglite";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { createEngine } from "../engine/engine";
import type { Engine } from "../engine/engine";
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

  // ---- UI serving --------------------------------------------------------

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

  // ---- list --------------------------------------------------------------

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

  // ---- detail ------------------------------------------------------------

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

  // ---- cancel / retry ------------------------------------------------------

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

  // ---- health --------------------------------------------------------------

  it("passes health through", async () => {
    const res = await get(dash, "/api/health");
    expect(res.status).toBe(200);
    const body = await readJson(res);
    expect(body.db).toBe(true);
    // This process never called listen(); health is per-process.
    expect(body.worker).toBe(false);
  });
});
