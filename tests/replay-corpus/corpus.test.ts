import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { PGlite } from "@electric-sql/pglite";
import { drizzle } from "drizzle-orm/pglite";
import { eq, sql } from "drizzle-orm";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { FlowRegistry } from "../../src/engine/registry";
import { playRunAttempt } from "../../src/engine/run-lifecycle";
import { silentLogger, baseRunnerDeps } from "../../src/engine/test-helpers";
import { createDrizzleStorage } from "../../src/storage/drizzle";
import type { WorkflowDb } from "../../src/storage/db";
import { runs, signals, steps, timers } from "../../src/storage/schema";
import { applyFlowSchema } from "../../src/storage/setup";
import { SCENARIOS } from "./scenarios";

const here = dirname(fileURLToPath(import.meta.url));

interface CapturedRows {
  runs: Record<string, unknown>[];
  steps: Record<string, unknown>[];
  timers: Record<string, unknown>[];
  signals: Record<string, unknown>[];
}

const snakeToCamel = (key: string): string => key.replace(/_([a-z])/g, (_, c) => c.toUpperCase());

const reviveDates = (row: Record<string, unknown>): Record<string, unknown> => {
  const out: Record<string, unknown> = {};
  for (const [k, v] of Object.entries(row)) {
    const camel = snakeToCamel(k);
    if (
      typeof v === "string" &&
      /^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}(?::?\d{2})?)?$/.test(v)
    ) {
      const normalized = v.replace(" ", "T").replace(/([+-]\d{2})$/, "$1:00");
      out[camel] = new Date(normalized);
    } else {
      out[camel] = v;
    }
  }
  return out;
};

const loadCorpus = (name: string): CapturedRows => {
  const raw = readFileSync(join(here, `${name}.json`), "utf8");
  const parsed = JSON.parse(raw) as CapturedRows;
  return {
    runs: parsed.runs.map(reviveDates),
    steps: parsed.steps.map(reviveDates),
    timers: parsed.timers.map(reviveDates),
    signals: parsed.signals.map(reviveDates),
  };
};

describe("replay corpus — captured snapshots survive the current engine", () => {
  let client: PGlite;
  let db: WorkflowDb;

  beforeEach(async () => {
    client = new PGlite();
    await client.waitReady;
    db = drizzle({ client }) as unknown as WorkflowDb;
    await applyFlowSchema(db);
  });
  afterEach(async () => {
    await client.close();
  });

  for (const sc of SCENARIOS) {
    it(`${sc.name}: replays to status=${sc.expected.status}`, async () => {
      const corpus = loadCorpus(sc.name);

      // 1) Insert the snapshot rows via raw drizzle so the engine sees a
      //    persisted suspension exactly as captured.
      for (const r of corpus.runs) {
        await db.insert(runs).values(r as typeof runs.$inferInsert);
      }
      for (const r of corpus.steps) await db.insert(steps).values(r as typeof steps.$inferInsert);
      for (const r of corpus.timers)
        await db.insert(timers).values(r as typeof timers.$inferInsert);
      for (const r of corpus.signals)
        await db.insert(signals).values(r as typeof signals.$inferInsert);

      const runId = (corpus.runs[0] as { id: string }).id;

      // 2) Resolve the suspension so playRunAttempt has work to do on resume.
      if (sc.resolveSuspend === "fire-timer") {
        await db
          .update(runs)
          .set({ status: "sleeping", updatedAt: new Date(0) })
          .where(eq(runs.id, runId));
        await db.execute(
          sql`UPDATE workflow.timers SET fired_at = NOW() WHERE run_id = ${runId}::uuid`,
        );
        await db.update(runs).set({ status: "pending" }).where(eq(runs.id, runId));
      } else if (sc.resolveSuspend === "deliver-signal") {
        await db.update(runs).set({ status: "awaiting_signal" }).where(eq(runs.id, runId));
        // Mark every armed signal as delivered with the test payload.
        await db
          .update(signals)
          .set({
            delivered: true,
            deliveredAt: new Date(),
            payload: sc.signal!.payload as object,
          })
          .where(eq(signals.runId, runId));
        await db.update(runs).set({ status: "pending" }).where(eq(runs.id, runId));
      } else {
        // already terminal — verify the loadOutput path
        const captured = (corpus.runs[0] as { status: string }).status;
        expect(captured).toBe("done");
      }

      // 3) Register the same flow that produced the snapshot.
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

      // 4) For terminal snapshots, just verify the output survives the round-trip.
      if (sc.resolveSuspend === "none") {
        const output = await storage.loadOutput(runId);
        expect(output).toEqual(sc.expected.output);
        return;
      }

      // 5) Otherwise replay and verify the outcome.
      const result = await playRunAttempt({ ...baseRunnerDeps(), registry, storage }, runId);
      expect(result.status).toBe(sc.expected.status);

      if (sc.expected.status === "completed") {
        const output = await storage.loadOutput(runId);
        expect(output).toEqual(sc.expected.output);
      }
    });
  }
});
