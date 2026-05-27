import { describe, expect, it } from "vitest";
import type { Pool } from "pg";
import type { Logger } from "./types";
import type { WorkflowDb } from "../storage/db";
import { createEngine } from "./engine";

const silent: Logger = {
  debug: () => undefined,
  info: () => undefined,
  warn: () => undefined,
  error: () => undefined,
};

const stubEngine = () =>
  createEngine({
    db: {} as unknown as WorkflowDb,
    pool: {} as unknown as Pool,
    logger: silent,
  });

describe("engine guards", () => {
  it("rejects user crons that collide with the reserved reconciler name", () => {
    const engine = stubEngine();
    expect(() =>
      engine.defineCron({
        name: "__iterativeflow_reconcile",
        schedule: "* * * * *",
        run: async () => undefined,
      }),
    ).toThrow(/reserved by the engine reconciler/);
  });

  it("accepts a normal cron name", () => {
    const engine = stubEngine();
    expect(() =>
      engine.defineCron({
        name: "user-cron",
        schedule: "* * * * *",
        run: async () => undefined,
      }),
    ).not.toThrow();
  });
});
