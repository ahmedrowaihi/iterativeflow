import { drizzle } from "drizzle-orm/node-postgres";
import { Pool } from "pg";
import { makeWorkerUtils } from "graphile-worker";
import { PostgreSqlContainer, type StartedPostgreSqlContainer } from "@testcontainers/postgresql";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { z } from "zod";
import { defineContract } from "../../builder/contract";
import { flow } from "../../builder/flow";
import { createEngine, type Engine } from "../../engine/engine";
import type { Logger } from "../../engine/types";
import { applyFlowSchema, dropFlowSchema } from "../../storage/setup";
import type { WorkflowDb } from "../../storage/db";

const externalUrl = process.env.ITERATIVE_PG_URL;
const skipContainers = process.env.SKIP_TESTCONTAINERS === "1";

const silent: Logger = {
  debug: () => undefined,
  info: () => undefined,
  warn: () => undefined,
  error: () => undefined,
};

const waitFor = async <T>(
  fn: () => Promise<T | undefined> | T | undefined,
  { timeoutMs = 15_000, intervalMs = 100 }: { timeoutMs?: number; intervalMs?: number } = {},
): Promise<T> => {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const v = await fn();
    if (v !== undefined) return v;
    await new Promise((r) => setTimeout(r, intervalMs));
  }
  throw new Error(`waitFor timed out after ${timeoutMs}ms`);
};

// LIGHT — the API process imports only this.
const cloneContract = defineContract<{ mediaId: string }, { status: "done" }>({
  name: "clone-media",
  version: 1,
  input: z.object({ mediaId: z.string() }),
});

// HEAVY — built from the same contract; lives with the worker.
const cloneFlow = flow(cloneContract)
  .step("copy", ({ input }) => ({ copied: input.mediaId }))
  .output(() => ({ status: "done" as const }))
  .build();

interface Harness {
  url: string;
  pools: Pool[];
  db: (pool: Pool) => WorkflowDb;
  cleanup: () => Promise<void>;
}

const setup = async (): Promise<Harness> => {
  let url: string;
  let container: StartedPostgreSqlContainer | undefined;
  if (externalUrl) {
    url = externalUrl;
  } else {
    container = await new PostgreSqlContainer("postgres:16-alpine").start();
    url = container.getConnectionUri();
  }
  const pools: Pool[] = [];
  return {
    url,
    pools,
    db: (pool) => drizzle({ client: pool }) as unknown as WorkflowDb,
    cleanup: async () => {
      for (const p of pools) await p.end();
      if (container) await container.stop();
    },
  };
};

describe.skipIf(skipContainers && !externalUrl)("enqueue-only handles (flow contract)", () => {
  let h: Harness;

  const newPool = (): Pool => {
    const p = new Pool({ connectionString: h.url });
    h.pools.push(p);
    return p;
  };

  beforeAll(async () => {
    h = await setup();
    const seed = newPool();
    await dropFlowSchema(h.db(seed)).catch(() => undefined);
    await applyFlowSchema(h.db(seed));
    const utils = await makeWorkerUtils({ pgPool: seed });
    await utils.release();
  }, 120_000);

  afterAll(async () => {
    if (!h) return;
    const first = h.pools[0];
    if (first) await dropFlowSchema(h.db(first)).catch(() => undefined);
    await h.cleanup();
  }, 60_000);

  it("a body-free enqueue from the contract runs on the worker that registered the body", async () => {
    const apiPool = newPool();
    const workerPool = newPool();

    // API engine: knows only the contract, registers no body, never listen()s.
    const api: Engine = createEngine({
      db: h.db(apiPool),
      pool: apiPool,
      logger: silent,
      reconciler: false,
    });
    // Worker engine: registers the full flow + listens.
    const worker: Engine = createEngine({
      db: h.db(workerPool),
      pool: workerPool,
      logger: silent,
      reconciler: false,
      worker: { concurrency: 2, pollInterval: 200 },
    });
    worker.register(cloneFlow);
    await worker.listen();

    try {
      // The API never registered a body → no worker, claims nothing.
      expect((await api.health()).worker).toBe(false);

      const handle = api.enqueueHandle(cloneContract);
      const { runId } = await handle.start({ mediaId: "m-42" });

      // The run lands under the per-flow identifier (ADR 0001) and only the
      // body-registered worker can claim it.
      const { rows } = await apiPool.query<{ task_identifier: string }>(
        "SELECT task_identifier FROM graphile_worker.jobs WHERE key = $1",
        [`flow:${runId}`],
      );
      expect(rows[0]?.task_identifier).toBe("flow:run:clone-media@1");

      const out = await handle.result(runId, { timeoutMs: 15_000 });
      expect(out).toEqual({ status: "done" });

      const final = await worker.status(runId);
      expect(final?.run.status).toBe("done");
    } finally {
      await api.stop().catch(() => undefined);
      await worker.stop().catch(() => undefined);
    }
  }, 60_000);

  it("the untyped escape hatch engine.enqueue() reaches the same worker", async () => {
    const apiPool = newPool();
    const workerPool = newPool();

    const api: Engine = createEngine({
      db: h.db(apiPool),
      pool: apiPool,
      logger: silent,
      reconciler: false,
    });
    const worker: Engine = createEngine({
      db: h.db(workerPool),
      pool: workerPool,
      logger: silent,
      reconciler: false,
      worker: { concurrency: 2, pollInterval: 200 },
    });
    worker.register(cloneFlow);
    await worker.listen();

    try {
      const { runId } = await api.enqueue("clone-media", 1, { mediaId: "m-99" });
      const done = await waitFor(
        async () => {
          const s = await worker.status(runId);
          return s?.run.status === "done" ? s : undefined;
        },
        { timeoutMs: 15_000 },
      );
      expect(done.run.status).toBe("done");
    } finally {
      await api.stop().catch(() => undefined);
      await worker.stop().catch(() => undefined);
    }
  }, 60_000);
});
