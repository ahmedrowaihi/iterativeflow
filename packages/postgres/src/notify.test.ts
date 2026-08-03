import { PostgreSqlContainer, type StartedPostgreSqlContainer } from "@testcontainers/postgresql";
import { createEngine, defineFlow } from "@iterativeflow/core";
import {
  applyNotifyTriggers,
  applyProgressTrigger,
  applySchema,
  createPgBackend,
  createPgListener,
  pgPool,
} from "@iterativeflow/postgres";
import { Pool } from "pg";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

const skip = process.env.SKIP_TESTCONTAINERS === "1";

/** Resolve true if `p` settles before `ms`, false otherwise — proves "woke via push, not poll". */
const settlesWithin = (p: Promise<unknown>, ms: number): Promise<boolean> =>
  Promise.race([p.then(() => true), new Promise<boolean>((r) => setTimeout(() => r(false), ms))]);

describe.skipIf(skip)("postgres LISTEN/NOTIFY push", () => {
  let container: StartedPostgreSqlContainer;
  let pool: Pool;

  beforeAll(async () => {
    container = await new PostgreSqlContainer("postgres:16-alpine").start();
    pool = new Pool({ connectionString: container.getConnectionUri() });
    pool.on("error", () => undefined);
    await applySchema(pgPool(pool));
    await applyNotifyTriggers(pgPool(pool));
    await applyProgressTrigger(pgPool(pool));
  }, 180_000);

  afterAll(async () => {
    await pool?.end().catch(() => undefined);
    await container?.stop().catch(() => undefined);
  });

  const listening = async (l: ReturnType<typeof createPgListener>): Promise<void> => {
    l.start();
    for (let i = 0; i < 200 && l.state() !== "listening"; i++) {
      await new Promise((r) => setTimeout(r, 10));
    }
  };

  it("waitForWork resolves on an enqueue from another connection (dispatch trigger)", async () => {
    const listener = createPgListener(pool);
    await listening(listener);
    const backend = createPgBackend(pgPool(pool));
    // waitForWork is armed with a huge timeout — only the enqueue's NOTIFY can settle it fast.
    const woke = settlesWithin(listener.waitForWork(30_000), 3_000);
    const { runId } = await backend.store.startRun({ name: "f", version: 1, input: {} });
    await backend.queue.enqueue(runId);
    expect(await woke).toBe(true);
    await listener.close();
  });

  it("wakeup.wait resolves when a run terminates (completion trigger, cross-connection)", async () => {
    const listener = createPgListener(pool);
    await listening(listener);
    const backend = createPgBackend(pgPool(pool));
    const { runId } = await backend.store.startRun({ name: "f", version: 1, input: {} });
    // markTerminal directly (not via the executor) so no local signal fires — only the DB
    // `done` trigger → listener can settle this wait.
    const woke = settlesWithin(listener.wakeup.wait(runId, 30_000), 3_000);
    await backend.store.markTerminal(runId, { status: "done", output: 1 });
    expect(await woke).toBe(true);
    await listener.close();
  });

  it("watch() streams a run's progress events across connections (progress trigger)", async () => {
    const listener = createPgListener(pool);
    await listening(listener);
    const backend = createPgBackend(pgPool(pool));
    const { runId } = await backend.store.startRun({ name: "p", version: 1, input: {} });

    // Subscribe FIRST, then insert events on the pool (another connection) — the trigger NOTIFYs
    // the progress channel and `watch` yields each, proving the cross-process push path.
    const seen: string[] = [];
    const collect = (async () => {
      for await (const ev of listener.watch(runId)) {
        seen.push(ev.type);
        if (seen.length === 2) break;
      }
    })();
    await new Promise((r) => setTimeout(r, 100)); // let the subscription register before inserting
    await pool.query(
      `INSERT INTO workflow.event (run_id, type, at, data)
       VALUES ($1, 'step.finished', now(), NULL), ($1, 'run.completed', now(), NULL)`,
      [runId],
    );

    expect(await settlesWithin(collect, 3_000)).toBe(true);
    expect(seen).toEqual(["step.finished", "run.completed"]);
    await listener.close();
  });

  it("push dispatch drives a run to done far faster than the poll tick", async () => {
    const listener = createPgListener(pool);
    await listening(listener);
    const backend = createPgBackend(pgPool(pool), { listener }); // wires BOTH push seams
    const flow = defineFlow<{ x: number }, number>({
      name: "double",
      version: 1,
      run: async (ctx, input) => ctx.step("d", () => input.x * 2),
    });
    const engine = createEngine(backend, [flow]);
    // tickMs 30s: pure polling would not claim the run for 30s. Push (off the backend) dispatches in ~ms.
    const stop = engine.run({ tickMs: 30_000, maintenanceMs: 30_000 });
    const handle = await engine.submit(flow, { x: 21 });
    const res = await engine.result(handle, { timeoutMs: 5_000 });
    expect(res).toMatchObject({ status: "done", output: 42 });
    await stop();
    await listener.close();
  });
});
