import { describe, expect, it } from "vitest";
import type { FlowNode } from "../builder/types";
import type { SignalRow, StepRow, TimerRow } from "../storage/schema";
import type { RunSnapshot } from "./types";
import { baseOf, checkCompat, createKeyCursor, signalBase, producibleKeys } from "./cursor";

const RUN_ID = "00000000-0000-0000-0000-000000000000";
const EPOCH = new Date(0);

const step = (name: string): FlowNode => ({ kind: "step", name, fn: () => undefined });
const sleep = (): FlowNode => ({ kind: "sleep", duration: 0 });
const hook = (name: string): FlowNode => ({ kind: "signal", name });
const loop = (...nodes: FlowNode[]): FlowNode => ({ kind: "loop", until: () => true, nodes });

const stepRow = (key: string): StepRow => ({
  runId: RUN_ID,
  cursorKey: key,
  status: "ok",
  result: null,
  error: null,
  attempts: 1,
  startedAt: EPOCH,
  completedAt: EPOCH,
});
const timerRow = (key: string): TimerRow => ({
  runId: RUN_ID,
  cursorKey: key,
  fireAt: EPOCH,
  firedAt: EPOCH,
});
const signalRow = (key: string): SignalRow => ({
  runId: RUN_ID,
  cursorKey: key,
  delivered: true,
  payload: null,
  expiresAt: null,
  createdAt: EPOCH,
  deliveredAt: EPOCH,
});

const snapshot = (
  keys: { steps?: string[]; timers?: string[]; signals?: string[] } = {},
): RunSnapshot => ({
  steps: new Map((keys.steps ?? []).map((k) => [k, stepRow(k)])),
  timers: new Map((keys.timers ?? []).map((k) => [k, timerRow(k)])),
  signals: new Map((keys.signals ?? []).map((k) => [k, signalRow(k)])),
});

describe("cursor / replay-key scheme", () => {
  describe("createKeyCursor", () => {
    it("returns the base on first call and suffixes from :1 onwards", () => {
      const c = createKeyCursor();
      expect(c.next("foo")).toBe("foo");
      expect(c.next("foo")).toBe("foo:1");
      expect(c.next("foo")).toBe("foo:2");
    });

    it("tracks each base independently", () => {
      const c = createKeyCursor();
      expect(c.next("a")).toBe("a");
      expect(c.next("b")).toBe("b");
      expect(c.next("a")).toBe("a:1");
      expect(c.next("b")).toBe("b:1");
    });

    it("treats hook prefixes as opaque bases", () => {
      const c = createKeyCursor();
      expect(c.next(signalBase("approve"))).toBe("signal:approve");
      expect(c.next(signalBase("approve"))).toBe("signal:approve:1");
    });
  });

  describe("baseOf", () => {
    it("returns the key unchanged when there is no suffix", () => {
      expect(baseOf("foo")).toBe("foo");
      expect(baseOf("signal:approve")).toBe("signal:approve");
    });

    it("strips a positive-integer suffix", () => {
      expect(baseOf("foo:1")).toBe("foo");
      expect(baseOf("foo:42")).toBe("foo");
      expect(baseOf("signal:approve:3")).toBe("signal:approve");
    });

    it("does not strip suffixes the cursor never emits (':0', ':01')", () => {
      expect(baseOf("foo:0")).toBe("foo:0");
      expect(baseOf("foo:01")).toBe("foo:01");
    });

    it("leaves non-numeric trailing segments alone", () => {
      expect(baseOf("signal:approve")).toBe("signal:approve");
      expect(baseOf("a:b")).toBe("a:b");
    });
  });

  describe("producibleKeys", () => {
    it("partitions step / sleep / hook keys into their bags", () => {
      const result = producibleKeys([step("a"), step("a"), sleep(), hook("done"), hook("done")]);
      expect(result).not.toBeNull();
      if (result === null) return;
      expect(result.step.keys).toEqual(new Set(["a", "a:1"]));
      expect(result.timer.keys).toEqual(new Set(["sleep"]));
      expect(result.signal.keys).toEqual(new Set(["signal:done", "signal:done:1"]));
    });

    it("records each bag's bases from the cursor, not from baseOf", () => {
      // Regression: baseOf("signal:42") wrongly strips digits → bases must come
      // from the cursor's input, not from re-parsing the emitted keys.
      const result = producibleKeys([hook("42"), hook("42")]);
      expect(result).not.toBeNull();
      if (result === null) return;
      expect(result.signal.bases).toEqual(new Set(["signal:42"]));
      expect(result.step.bases.size).toBe(0);
      expect(result.timer.bases.size).toBe(0);
    });

    it("partitions loop-body bases into loopBases (dynamic) vs bases (fixed)", () => {
      const result = producibleKeys([step("a"), loop(step("inner"))]);
      expect(result).not.toBeNull();
      if (result === null) return;
      expect(result.step.bases).toEqual(new Set(["a"]));
      expect(result.step.loopBases).toEqual(new Set(["inner"]));
      expect(result.step.keys).toEqual(new Set(["a"]));
    });
  });

  describe("checkCompat", () => {
    it("returns null when every recorded key matches its bag", () => {
      const ok = snapshot({ steps: ["a", "a:1"], timers: ["sleep"], signals: ["signal:done"] });
      expect(checkCompat(ok, [step("a"), step("a"), sleep(), hook("done")])).toBeNull();
    });

    it("still detects renames/kind-changes inside loop bodies (REPLAY_INCOMPATIBLE_VERSION)", () => {
      const result = checkCompat(snapshot({ steps: ["alien"] }), [loop(step("inner"))]);
      expect(result?.code).toBe("REPLAY_INCOMPATIBLE_VERSION");
    });

    it("does not flag a recorded loop-body key whose base is still produced", () => {
      // Snapshot recorded "inner:5" (loop body iteration). Current graph still
      // contains a loop with step "inner" — base matches even though the
      // concrete key is dynamic.
      expect(checkCompat(snapshot({ steps: ["inner:5"] }), [loop(step("inner"))])).toBeNull();
    });

    it("flags REPLAY_NON_DETERMINISTIC when occurrence count for a base shrinks", () => {
      const result = checkCompat(snapshot({ steps: ["a", "a:1"] }), [step("a")]);
      expect(result?.code).toBe("REPLAY_NON_DETERMINISTIC");
      expect(result?.message).toContain("a:1");
    });

    it("flags REPLAY_INCOMPATIBLE_VERSION when a base name disappears entirely", () => {
      const result = checkCompat(snapshot({ steps: ["original"] }), [step("renamed")]);
      expect(result?.code).toBe("REPLAY_INCOMPATIBLE_VERSION");
      expect(result?.message).toContain("original");
    });

    it("flags REPLAY_INCOMPATIBLE_VERSION when a step row's kind switched to sleep", () => {
      // Previously this passed silently and the step result was lost on replay.
      const result = checkCompat(snapshot({ steps: ["sleep"] }), [sleep()]);
      expect(result?.code).toBe("REPLAY_INCOMPATIBLE_VERSION");
      expect(result?.message).toContain("step");
    });

    it("flags REPLAY_INCOMPATIBLE_VERSION when a step name collides with a hook key", () => {
      const result = checkCompat(snapshot({ steps: ["signal:approve"] }), [hook("approve")]);
      expect(result?.code).toBe("REPLAY_INCOMPATIBLE_VERSION");
    });

    it("classifies a renamed numeric hook as REPLAY_INCOMPATIBLE_VERSION", () => {
      // baseOf("signal:42") would strip ":42"; bag.bases comes from the cursor,
      // so {"signal:99"} doesn't match "signal:42" → rename, not count drift.
      const result = checkCompat(snapshot({ signals: ["signal:42"] }), [hook("99")]);
      expect(result?.code).toBe("REPLAY_INCOMPATIBLE_VERSION");
    });

    it("flags REPLAY_NON_DETERMINISTIC when a numeric-hook's occurrence count shrinks", () => {
      const result = checkCompat(snapshot({ signals: ["signal:42:1"] }), [hook("42")]);
      expect(result?.code).toBe("REPLAY_NON_DETERMINISTIC");
    });
  });
});
