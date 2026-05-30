import { describe, expect, it, vi } from "vitest";
import type { Logger } from "../engine/types";
import { wrapLogger } from "./safe-logger";

describe("wrapLogger", () => {
  it("forwards non-throwing methods unchanged", () => {
    const calls: { level: string; msg: string }[] = [];
    const base: Logger = {
      debug: (m) => calls.push({ level: "debug", msg: m }),
      info: (m) => calls.push({ level: "info", msg: m }),
      warn: (m) => calls.push({ level: "warn", msg: m }),
      error: (e) => calls.push({ level: "error", msg: e.message }),
    };
    const safe = wrapLogger(base);
    safe.info("hi");
    safe.warn("watch");
    expect(calls).toEqual([
      { level: "info", msg: "hi" },
      { level: "warn", msg: "watch" },
    ]);
  });

  it("suppresses a throwing method without crashing and warns once via stderr", () => {
    const stderr = vi.spyOn(console, "error").mockImplementation(() => undefined);
    const base: Logger = {
      debug: () => undefined,
      info: () => {
        throw new Error("user logger boom");
      },
      warn: () => undefined,
      error: () => undefined,
    };
    const safe = wrapLogger(base);
    expect(() => safe.info("x")).not.toThrow();
    expect(() => safe.info("y")).not.toThrow();
    expect(() => safe.info("z")).not.toThrow();
    expect(stderr).toHaveBeenCalledTimes(1);
    expect(stderr.mock.calls[0][0]).toContain("logger.info threw");
    stderr.mockRestore();
  });
});
