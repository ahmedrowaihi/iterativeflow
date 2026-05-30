#!/usr/bin/env tsx
import { mkdirSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { PGlite } from "@electric-sql/pglite";
import { drizzle } from "drizzle-orm/pglite";
import { sql } from "drizzle-orm";
import { FlowRegistry } from "../src/engine/registry";
import { playRunAttempt } from "../src/engine/run-lifecycle";
import { silentLogger, baseRunnerDeps } from "../src/engine/test-helpers";
import { createDrizzleStorage } from "../src/storage/drizzle";
import type { WorkflowDb } from "../src/storage/db";
import { applyFlowSchema } from "../src/storage/setup";
import { SCENARIOS } from "../tests/replay-corpus/scenarios";

const scriptDir = dirname(fileURLToPath(import.meta.url));
const OUT = join(scriptDir, "..", "tests", "replay-corpus");

mkdirSync(OUT, { recursive: true });

const dumpRows = async (db: WorkflowDb, runId: string) => {
  const exec = async <T>(query: ReturnType<typeof sql>): Promise<T[]> => {
    const r = (await db.execute(query)) as unknown as { rows: T[] };
    return r.rows;
  };
  return {
    runs: await exec(sql`SELECT * FROM workflow.runs WHERE id = ${runId}::uuid`),
    steps: await exec(sql`SELECT * FROM workflow.steps WHERE run_id = ${runId}::uuid`),
    timers: await exec(sql`SELECT * FROM workflow.timers WHERE run_id = ${runId}::uuid`),
    signals: await exec(sql`SELECT * FROM workflow.signals WHERE run_id = ${runId}::uuid`),
  };
};

for (const sc of SCENARIOS) {
  const client = new PGlite();
  await client.waitReady;
  const db = drizzle({ client }) as unknown as WorkflowDb;
  await applyFlowSchema(db);

  const registry = new FlowRegistry();
  registry.register({
    name: sc.def.name,
    version: sc.def.version,
    run: sc.def.body,
    nodes: sc.def.nodes,
  });
  const storage = createDrizzleStorage({
    db,
    logger: silentLogger,
    enqueue: async () => undefined,
  });

  const { runId } = await storage.createRun({
    name: sc.def.name,
    version: sc.def.version,
    input: {},
  });

  if (sc.resolveSuspend !== "none") {
    // Run once: produces the suspension (sleep / awaiting_signal).
    await playRunAttempt({ ...baseRunnerDeps(), registry, storage }, runId);
  } else {
    // Drive to completion so the captured snapshot is a fully-terminal run.
    await playRunAttempt({ ...baseRunnerDeps(), registry, storage }, runId);
  }

  const rows = await dumpRows(db, runId);
  writeFileSync(join(OUT, `${sc.name}.json`), `${JSON.stringify(rows, null, 2)}\n`, "utf8");
  console.log(`captured ${sc.name}.json (status=${(rows.runs[0] as { status: string }).status})`);

  await client.close();
}
