import { flow } from "../../src/builder/flow";
import type { FlowDefinition } from "../../src/builder/types";

/**
 * Each entry is one captured suspension point. The flow definition lives here
 * (in code), and the corresponding row snapshot lives in `<name>.json` (on
 * disk). The corpus test loads the JSON, inserts it into pglite, registers the
 * flow defined here, drives the suspend resolution, and verifies the run
 * reaches the documented terminal state.
 *
 * When the storage shape changes, regenerate via `tsx scripts/capture-corpus.ts`.
 */

export interface CorpusScenario {
  /** File-system name (matches `<name>.json` in this directory). */
  name: string;
  /** The flow definition that originally produced the snapshot. */
  def: FlowDefinition<unknown, unknown>;
  /** Action to take between loading the snapshot and replaying the run. */
  resolveSuspend: "fire-timer" | "deliver-signal" | "none";
  /** Signal name + payload to deliver when `resolveSuspend === "deliver-signal"`. */
  signal?: { name: string; payload: unknown };
  /** Expected outcome of the replayed playRunAttempt. */
  expected: {
    status: "completed" | "suspended" | "failed";
    output?: unknown;
  };
}

const sleepSuspended = flow("corpus-sleep")
  .step("first", () => 1)
  .sleep("1h")
  .step("second", ({ input }) => (input as number) + 1)
  .output(({ input }) => input)
  .build();

const signalSuspended = flow("corpus-signal")
  .step("compute", () => 42)
  .signal<{ token: string }>("approve")
  .output(() => "approved")
  .build();

const completedRun = flow("corpus-completed")
  .step("a", () => "alpha")
  .step("b", ({ input }) => `${input}-beta`)
  .output(({ input }) => input)
  .build();

export const SCENARIOS: ReadonlyArray<CorpusScenario> = [
  {
    name: "sleep-suspended",
    def: sleepSuspended,
    resolveSuspend: "fire-timer",
    expected: { status: "completed", output: 2 },
  },
  {
    name: "signal-suspended",
    def: signalSuspended,
    resolveSuspend: "deliver-signal",
    signal: { name: "approve", payload: { token: "ok" } },
    expected: { status: "completed", output: "approved" },
  },
  {
    name: "completed-run",
    def: completedRun,
    resolveSuspend: "none",
    expected: { status: "completed", output: "alpha-beta" },
  },
];
