import { describe, expect, it } from "vitest";
import { toFireAt, toMs } from "./duration";

describe("duration", () => {
  describe("toMs", () => {
    it("passes through positive numbers", () => {
      expect(toMs(1_500)).toBe(1_500);
      expect(toMs(0)).toBe(0);
    });

    it("parses string durations", () => {
      expect(toMs("5s")).toBe(5_000);
      expect(toMs("1m")).toBe(60_000);
      expect(toMs("2h")).toBe(7_200_000);
    });

    it("clamps past Date values to 0 (already-past sleep targets)", () => {
      expect(toMs(new Date(Date.now() - 60_000))).toBe(0);
    });

    it("throws on negative numbers", () => {
      expect(() => toMs(-100)).toThrow(/non-negative/);
    });

    it("throws on negative duration strings", () => {
      expect(() => toMs("-5m")).toThrow(/non-negative/);
    });

    it("throws on invalid strings", () => {
      expect(() => toMs("zzz" as unknown as Parameters<typeof toMs>[0])).toThrow(
        /Invalid duration/,
      );
    });
  });

  describe("toFireAt", () => {
    it("returns Date as-is", () => {
      const d = new Date(2030, 0, 1);
      expect(toFireAt(d)).toBe(d);
    });

    it("converts a positive duration into a future Date", () => {
      const before = Date.now();
      const result = toFireAt("1s");
      expect(result.getTime()).toBeGreaterThanOrEqual(before + 1_000);
    });

    it("propagates the negative-throw from toMs", () => {
      expect(() => toFireAt(-100)).toThrow(/non-negative/);
    });
  });
});
