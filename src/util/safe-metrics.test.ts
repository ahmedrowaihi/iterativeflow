import { describe, expect, it } from "vitest";
import { silentLogger } from "../engine/test-helpers";
import type { Logger, MetricsRecorder } from "../engine/types";
import { wrapMetrics } from "./safe-metrics";

describe("wrapMetrics", () => {
  it("forwards a working metric method", () => {
    const seen: unknown[] = [];
    const base: MetricsRecorder = {
      runStarted: (p) => seen.push(p),
    };
    const safe = wrapMetrics(base, silentLogger);
    safe.runStarted?.({ name: "f", version: 1 });
    expect(seen).toEqual([{ name: "f", version: 1 }]);
  });

  it("swallows a throwing metric and warns once via the logger", () => {
    const warns: { msg: string; payload?: Record<string, unknown> }[] = [];
    const logger: Logger = {
      debug: () => undefined,
      info: () => undefined,
      warn: (msg, payload) => warns.push({ msg, payload }),
      error: () => undefined,
    };
    const base: MetricsRecorder = {
      runStarted: () => {
        throw new Error("metric boom");
      },
    };
    const safe = wrapMetrics(base, logger);
    expect(() => safe.runStarted?.({ name: "f", version: 1 })).not.toThrow();
    expect(() => safe.runStarted?.({ name: "f", version: 1 })).not.toThrow();
    expect(warns).toEqual([
      {
        msg: "flow.metrics.threw",
        payload: { method: "runStarted", message: "metric boom" },
      },
    ]);
  });

  it("omits methods the recorder doesn't define", () => {
    const safe = wrapMetrics({}, silentLogger);
    expect(safe.runStarted).toBeUndefined();
  });
});
