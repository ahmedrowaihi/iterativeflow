import { describe, expect, it } from "vitest";
import { RUN_STATUSES as SCHEMA_RUN_STATUSES } from "./schema";
import { ACTIVE_RUN_STATUSES, RUN_STATUSES } from "./run-statuses";

describe("run status parity", () => {
  it("mirrors RUN_STATUSES from schema.ts", () => {
    expect([...RUN_STATUSES]).toEqual([...SCHEMA_RUN_STATUSES]);
  });

  it("marks every non-terminal state active", () => {
    const active = RUN_STATUSES.filter((s) => ACTIVE_RUN_STATUSES.has(s));
    expect(active).toEqual(["pending", "running", "sleeping", "awaiting_signal", "retrying"]);
  });
});
