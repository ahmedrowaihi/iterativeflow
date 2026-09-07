import { describe, expect, it } from "vitest";
import { nextCronAfter, parseCron } from "#engine/cron";

describe("cron expression parser", () => {
  it("computes the next fire for a daily schedule", () => {
    const next = nextCronAfter("0 0 * * *", new Date("2030-03-15T08:30:00Z"));
    expect(next.toISOString()).toBe("2030-03-16T00:00:00.000Z");
  });

  it("handles step and range fields", () => {
    const next = nextCronAfter("*/15 9-17 * * *", new Date("2030-03-15T09:07:00Z"));
    expect(next.toISOString()).toBe("2030-03-15T09:15:00.000Z");
  });

  it("applies the OR rule when day-of-month and day-of-week are both restricted", () => {
    // 1st of the month OR any Monday. 2030-03-15 is a Friday; next is Monday the 18th.
    const next = nextCronAfter("0 0 1 * 1", new Date("2030-03-15T00:00:00Z"));
    expect(next.getUTCDay()).toBe(1); // Monday
  });

  it("rejects malformed expressions", () => {
    expect(() => parseCron("* * *")).toThrow();
    expect(() => parseCron("60 * * * *")).toThrow();
    expect(() => parseCron("* 25 * * *")).toThrow();
  });

  it("treats `n/step` as a range from n to the field max, like Vixie cron", () => {
    // was silently parsed as the single value {0} — i.e. hourly instead of every 15 minutes
    const at = (iso: string) => nextCronAfter("0/15 * * * *", new Date(iso));
    expect(at("2030-01-01T00:00:00Z")?.toISOString()).toBe("2030-01-01T00:15:00.000Z");
    expect(at("2030-01-01T00:16:00Z")?.toISOString()).toBe("2030-01-01T00:30:00.000Z");
    expect(at("2030-01-01T00:46:00Z")?.toISOString()).toBe("2030-01-01T01:00:00.000Z");
  });
});
