import { drizzle } from "drizzle-orm/node-postgres";
import { Pool } from "pg";
import { makeWorkerUtils } from "graphile-worker";
import { PostgreSqlContainer, type StartedPostgreSqlContainer } from "@testcontainers/postgresql";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
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

const alpha = flow("alpha")
  .step("a", () => "alpha-done")
  .output(() => "alpha-done")
  .build();

const beta = flow("beta")
  .step("b", () => "beta-done")
  .output(() => "beta-done")
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

describe.skipIf(skipContainers && !externalUrl)("per-flow task routing", () => {
  let h: Harness;

  const newPool = (): Pool => {
    const p = new Pool({ connectionString: h.url });
    h.pools.push(p);
    return p;
  };

  beforeAll(async () => {
    h = await setup();
    const seed = newPool();
    const seedDb = h.db(seed);
    await dropFlowSchema(seedDb).catch(() => undefined);
    await applyFlowSchema(seedDb);
    const utils = await makeWorkerUtils({ pgPool: seed });
    await utils.release();
  }, 120_000);

  afterAll(async () => {
    if (!h) return;
    const last = h.pools[0];
    if (last) await dropFlowSchema(h.db(last)).catch(() => undefined);
    await h.cleanup();
  }, 60_000);

  it("a worker claims only flows it registered — no cross-claim failure", async () => {
    const poolA = newPool();
    const poolB = newPool();
    const poolApi = newPool();

    // Worker A: alpha only. Worker B: beta only.
    const engineA: Engine = createEngine({
      db: h.db(poolA),
      pool: poolA,
      logger: silent,
      reconciler: false,
      worker: { concurrency: 2, pollInterval: 200 },
    });
    const engineB: Engine = createEngine({
      db: h.db(poolB),
      pool: poolB,
      logger: silent,
      reconciler: false,
      worker: { concurrency: 2, pollInterval: 200 },
    });
    // API: registers both for `.start` handles, never listen()s.
    const engineApi: Engine = createEngine({
      db: h.db(poolApi),
      pool: poolApi,
      logger: silent,
      reconciler: false,
    });

    const handleAlphaA = engineA.register(alpha);
    engineB.register(beta);
    const handleAlphaApi = engineApi.register(alpha);
    const handleBetaApi = engineApi.register(beta);

    await engineA.listen();
    await engineB.listen();

    try {
      // Enqueue-only API shape: registers both, no worker.
      expect((await engineApi.health()).worker).toBe(false);

      // alpha started from the API must land on worker A and reach done.
      const { runId: alphaRun } = await handleAlphaApi.start({});
      const alphaOut = await handleAlphaA.result(alphaRun, { timeoutMs: 15_000 });
      expect(alphaOut).toBe("alpha-done");

      // beta started from the API must land on worker B and reach done.
      const { runId: betaRun } = await handleBetaApi.start({});
      const betaFinal = await waitFor(
        async () => {
          const s = await engineB.status(betaRun);
          return s?.run.status === "done" ? s : undefined;
        },
        { timeoutMs: 15_000 },
      );
      expect(betaFinal.run.status).toBe("done");

      // Neither run failed with the cross-claim "No flow registered" error.
      const alphaStatus = await engineA.status(alphaRun);
      expect(alphaStatus?.run.status).toBe("done");
      expect(alphaStatus?.run.error).toBeFalsy();
      expect(betaFinal.run.error).toBeFalsy();
    } finally {
      await engineA.stop().catch(() => undefined);
      await engineB.stop().catch(() => undefined);
    }
  }, 60_000);

  it("monolith registers and listens both flows — both run to completion", async () => {
    const pool = newPool();
    const engine: Engine = createEngine({
      db: h.db(pool),
      pool,
      logger: silent,
      reconciler: false,
      worker: { concurrency: 2, pollInterval: 200 },
    });
    const handleAlpha = engine.register(alpha);
    const handleBeta = engine.register(beta);
    await engine.listen();

    try {
      const { runId: ra } = await handleAlpha.start({});
      const { runId: rb } = await handleBeta.start({});
      expect(await handleAlpha.result(ra, { timeoutMs: 15_000 })).toBe("alpha-done");
      expect(await handleBeta.result(rb, { timeoutMs: 15_000 })).toBe("beta-done");
    } finally {
      await engine.stop().catch(() => undefined);
    }
  }, 60_000);
});
