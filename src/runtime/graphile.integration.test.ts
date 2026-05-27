import { drizzle } from "drizzle-orm/node-postgres";
import { Pool } from "pg";
import { makeWorkerUtils } from "graphile-worker";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { flow } from "../builder/flow";
import { createEngine } from "../engine/engine";
import type { Logger } from "../engine/types";
import { applyWorkflowSchema, dropWorkflowSchema } from "../storage/setup";
import type { WorkflowDb } from "../storage/db";

const url = process.env.ITERATIVE_PG_URL;

const silent: Logger = {
  debug: () => undefined,
  info: () => undefined,
  warn: () => undefined,
  error: () => undefined,
};

describe.skipIf(!url)("real-pg smoke (set ITERATIVE_PG_URL to run)", () => {
  const pool = new Pool({ connectionString: url });
  const db = drizzle({ client: pool }) as unknown as WorkflowDb;

  beforeAll(async () => {
    await dropWorkflowSchema(db).catch(() => undefined);
    await applyWorkflowSchema(db);
    const utils = await makeWorkerUtils({ pgPool: pool });
    await utils.release();
  });

  afterAll(async () => {
    await dropWorkflowSchema(db).catch(() => undefined);
    await pool.end();
  });

  it("start() inserts a graphile_worker.add_job inside the outbox txn", async () => {
    const engine = createEngine({
      db,
      pool,
      logger: silent,
      disableReconciler: true,
    });

    const handle = engine.register(
      flow("smoke")
        .step("noop", () => "ok")
        .build(),
    );

    const { runId } = await handle.start({});

    const { rows } = await pool.query<{ key: string }>(
      "SELECT key FROM graphile_worker.jobs WHERE key = $1",
      [`workflow:${runId}`],
    );
    expect(rows).toHaveLength(1);
    expect(rows[0].key).toBe(`workflow:${runId}`);

    await pool.query("DELETE FROM graphile_worker.jobs WHERE key = $1", [`workflow:${runId}`]);
  });
});
