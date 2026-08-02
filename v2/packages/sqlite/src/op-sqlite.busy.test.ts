import { describe, expect, it } from "vitest";
import { opSqliteDb } from "#op-sqlite";

// A fake op-sqlite driver whose `BEGIN IMMEDIATE` fails `failBegins` times before succeeding, so the
// adapter's busy-retry loop can be exercised without a real database. `begins` counts attempts.
const makeDriver = (failBegins: number, error = "SQLITE_BUSY: database is locked") => {
  let begins = 0;
  return {
    get begins() {
      return begins;
    },
    execute(sql: string) {
      if (/^\s*BEGIN/i.test(sql)) {
        begins++;
        if (begins <= failBegins) throw new Error(error);
      }
      return { rows: [] as Record<string, unknown>[] };
    },
  };
};

describe("opSqliteDb busy retry", () => {
  it("retries BEGIN on SQLITE_BUSY, then commits", async () => {
    const driver = makeDriver(2);
    const out = await opSqliteDb(driver).tx(async (t) => {
      await t.query("SELECT 1");
      return "ok";
    });
    expect(out).toBe("ok");
    expect(driver.begins).toBe(3); // 2 busy + 1 success
  });

  it("surfaces the busy error after exhausting retries", async () => {
    const driver = makeDriver(Number.POSITIVE_INFINITY);
    await expect(opSqliteDb(driver).tx(async () => undefined)).rejects.toThrow(/SQLITE_BUSY/);
    expect(driver.begins).toBe(6); // initial attempt + 5 retries
  });

  it("does not retry a non-busy error", async () => {
    const driver = makeDriver(Number.POSITIVE_INFINITY, "no such table: foo");
    await expect(opSqliteDb(driver).tx(async () => undefined)).rejects.toThrow(/no such table/);
    expect(driver.begins).toBe(1);
  });
});
