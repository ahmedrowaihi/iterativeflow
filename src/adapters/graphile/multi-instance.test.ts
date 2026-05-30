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

interface MultiHarness {
  url: string;
  poolA: Pool;
  poolB: Pool;
  dbA: WorkflowDb;
  dbB: WorkflowDb;
  cleanup: () => Promise<void>;
}

const setup = async (): Promise<MultiHarness> => {
  let url: string;
  let container: StartedPostgreSqlContainer | undefined;
  if (externalUrl) {
    url = externalUrl;
  } else {
    container = await new PostgreSqlContainer("postgres:16-alpine").start();
    url = container.getConnectionUri();
  }
  const poolA = new Pool({ connectionString: url });
  const poolB = new Pool({ connectionString: url });
  return {
    url,
    poolA,
    poolB,
    dbA: drizzle({ client: poolA }) as unknown as WorkflowDb,
    dbB: drizzle({ client: poolB }) as unknown as WorkflowDb,
    cleanup: async () => {
      await poolA.end();
      await poolB.end();
      if (container) await container.stop();
    },
  };
};

const waitFor = async <T>(
  fn: () => Promise<T | undefined> | T | undefined,
  { timeoutMs = 10_000, intervalMs = 100 }: { timeoutMs?: number; intervalMs?: number } = {},
): Promise<T> => {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const v = await fn();
    if (v !== undefined) return v;
    await new Promise((r) => setTimeout(r, intervalMs));
  }
  throw new Error(`waitFor timed out after ${timeoutMs}ms`);
};

describe.skipIf(skipContainers && !externalUrl)("multi-instance (two engines, same DB)", () => {
  let h: MultiHarness;
  let engineA: Engine;
  let engineB: Engine;

  beforeAll(async () => {
    h = await setup();
    await dropFlowSchema(h.dbA).catch(() => undefined);
    await applyFlowSchema(h.dbA);
    const utils = await makeWorkerUtils({ pgPool: h.poolA });
    await utils.release();
    engineA = createEngine({
      db: h.dbA,
      pool: h.poolA,
      logger: silent,
      disableReconciler: true,
      concurrency: 2,
      pollInterval: 200,
    });
    engineB = createEngine({
      db: h.dbB,
      pool: h.poolB,
      logger: silent,
      disableReconciler: true,
      concurrency: 2,
      pollInterval: 200,
    });
    await engineA.listen();
    await engineB.listen();
  }, 120_000);

  afterAll(async () => {
    if (!h) return;
    await engineA?.stop().catch(() => undefined);
    await engineB?.stop().catch(() => undefined);
    await dropFlowSchema(h.dbA).catch(() => undefined);
    await h.cleanup();
  }, 60_000);

  it("handle.result() on engine B wakes when the run terminates on engine A", async () => {
    const def = flow("xinst-terminal")
      .step("compute", () => 42)
      .output(({ input }) => input)
      .build();
    const handleA = engineA.register(def);
    const handleB = engineB.register(def);

    const { runId } = await handleA.start({});
    // Engine B blocks on result() — the only way it learns about completion
    // is via `LISTEN flow_terminal` notifications fired from engine A's worker.
    const output = await handleB.result(runId, { timeoutMs: 15_000 });
    expect(output).toBe(42);
  }, 30_000);

  it("handle.wait({ until: { step } }) on engine B wakes when engine A finishes the step", async () => {
    const def = flow("xinst-progress")
      .step("first", () => "ok")
      .sleep("500ms")
      .step("second", () => "done")
      .build();
    const handleA = engineA.register(def);
    const handleB = engineB.register(def);

    const { runId } = await handleA.start({});
    await handleB.wait(runId, { until: { step: "first" }, timeoutMs: 15_000 });
    // After the wait resolves, the step row must already be persisted by A.
    const status = await engineB.status(runId);
    const stepRow = status?.steps.find((s) => s.cursorKey === "first");
    expect(stepRow?.status).toBe("ok");
  }, 30_000);

  it("engine.signal() on engine B unblocks a run armed by engine A's worker", async () => {
    const def = flow("xinst-signal")
      .signal("approve")
      .output(() => "approved")
      .build();
    const handleA = engineA.register(def);
    engineB.register(def); // engine B must know the flow to drive the resumed run

    const { runId } = await handleA.start({});
    // Wait until A's worker has armed the signal row (status flips to awaiting_signal).
    await waitFor(
      async () => {
        const s = await engineB.status(runId);
        return s?.run.status === "awaiting_signal" ? s : undefined;
      },
      { timeoutMs: 15_000 },
    );
    // Engine B delivers the signal; engine A's worker (or B's) picks the
    // re-enqueued job and drives the run to completion.
    const result = await engineB.signal(runId, "approve", {});
    expect(result.kind).toBe("delivered");

    const output = await handleA.result(runId, { timeoutMs: 15_000 });
    expect(output).toBe("approved");
  }, 30_000);

  it("engine.cancel() on engine B terminates a sleeping run started on engine A", async () => {
    const def = flow("xinst-cancel")
      .step("seed", () => "ok")
      .sleep("60s")
      .step("never-runs", () => "no")
      .build();
    const handleA = engineA.register(def);
    engineB.register(def); // engine B must know the flow to act on the run

    const { runId } = await handleA.start({});
    // Wait for the run to actually enter sleep (so cancel hits a sleeping row, not a pending one).
    await waitFor(
      async () => {
        const s = await engineB.status(runId);
        return s?.run.status === "sleeping" ? s : undefined;
      },
      { timeoutMs: 15_000 },
    );
    await engineB.cancel(runId, "test-cancel");

    const final = await waitFor(
      async () => {
        const s = await engineB.status(runId);
        return s?.run.status === "canceled" ? s : undefined;
      },
      { timeoutMs: 10_000 },
    );
    expect(final.run.error?.code).toBe("RUN_CANCELED");
  }, 45_000);
});
