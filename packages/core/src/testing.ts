/**
 * The test harness (`@iterativeflow/core/testing`) — a virtual clock over a real engine, so a flow
 * that sleeps for three days settles in a millisecond. Time only moves when you move it, and every
 * jump lands exactly on a durable deadline, so there is nothing to poll and nothing to guess.
 *
 * It drives the same {@link Engine} against any {@link Backend}: the in-memory one for unit tests,
 * or a real database for an integration test that still doesn't wait out a sleep.
 *
 * @packageDocumentation
 */

import { type Engine, type EngineOpts, createEngine } from "#engine/engine";
import type { AnyFlow, OutputSchema } from "#engine/flow";
import type { RunHandle, RunResult } from "#engine/worker";
import type { Backend } from "#ports/outbox";
import { isTerminal } from "#status";

/** Ceilings that turn a non-progressing flow into a thrown error instead of a hung test. */
const MAX_TICKS_PER_DRAIN = 1_000;
const MAX_WAKES_PER_SETTLE = 1_000;

/** A virtual-time engine plus the controls to move its clock. Build one with {@link createTestHarness}. */
export interface TestHarness {
  /** The engine under test — submit, signal, cancel and query it exactly as in production. */
  readonly engine: Engine;

  /** The current virtual instant. Only {@link TestHarness.advance} and
   *  {@link TestHarness.advanceToNextWake} move it. */
  now(): Date;

  /** Run ticks until a tick claims nothing, without moving the clock. Returns how many runs executed. */
  drain(): Promise<number>;

  /** Move the clock forward by `ms`, then drain. Returns how many runs executed. */
  advance(ms: number): Promise<number>;

  /**
   * Drain, then jump to the next durable deadline — the earliest pending sleep, retry backoff or
   * cron — and drain again. Draining first is what makes the deadline knowable: a freshly submitted
   * run has no timer until it executes far enough to park. `false` when nothing is scheduled after
   * that, which means no amount of waiting would advance the engine. The clock never moves
   * backwards, so a deadline already in the past just drains.
   */
  advanceToNextWake(): Promise<boolean>;

  /**
   * Drive `handle` to its terminal outcome, jumping across every sleep and backoff on the way.
   *
   * @throws {Error} if the run parks with no deadline to jump to — waiting on a signal or a child
   * that nothing will deliver. The message names what it is waiting on, because that is nearly
   * always a missing `engine.signal(...)` in the test rather than a bug in the flow. The output is
   * `unknown` unless you pass the flow's `output` schema, exactly as with `engine.result`.
   */
  settle(handle: string): Promise<RunResult<unknown>>;
  settle<O>(
    handle: RunHandle<O> | string,
    opts: { output: OutputSchema<O> },
  ): Promise<RunResult<O>>;
}

/**
 * Build a {@link TestHarness} over `backend`, running `flows` on a clock that starts at `startAt`
 * (default `2030-01-01T00:00:00Z` — a fixed instant, so snapshots and log lines are stable).
 *
 * `opts` takes the usual {@link EngineOpts} minus `now`, which the harness owns.
 *
 * ```ts
 * const t = createTestHarness(createMemoryBackend(), [onboard]);
 * const handle = await t.engine.submit(onboard, { userId: "u_1" });
 * await t.advanceToNextWake();                          // the 3-day sleep
 * await t.engine.signal(handle, "survey", { score: 9 });
 * expect(await t.settle(handle)).toMatchObject({ status: "done" });
 * ```
 */
export const createTestHarness = (
  backend: Backend,
  flows: readonly AnyFlow[],
  opts: Omit<EngineOpts, "now"> & { startAt?: Date } = {},
): TestHarness => {
  const { startAt, ...engineOpts } = opts;
  let at = startAt ?? new Date("2030-01-01T00:00:00Z");
  const engine = createEngine(backend, flows, { ...engineOpts, now: () => at });

  const drain = async (): Promise<number> => {
    let executed = 0;
    for (let i = 0; i < MAX_TICKS_PER_DRAIN; i++) {
      const results = await engine.tick();
      if (results.length === 0) return executed;
      executed += results.length;
    }
    throw new Error(
      `drain: still claiming work after ${MAX_TICKS_PER_DRAIN} ticks — a flow is looping without parking`,
    );
  };

  const advanceTo = async (instant: Date): Promise<number> => {
    if (instant.getTime() > at.getTime()) at = instant;
    return await drain();
  };

  const stallReason = async (runId: string): Promise<string> => {
    const snap = await engine.status(runId);
    if (!snap) return `run ${runId} is gone`;
    const { status } = snap.run;
    if (status === "awaiting_signal")
      return `run ${runId} is awaiting a signal with no pending timer — deliver it with engine.signal(handle, name, payload)`;
    if (status === "awaiting_child") {
      const live = (await backend.store.childrenOf(runId)).filter((c) => !isTerminal(c.status));
      const blocking = live.map((c) => `${c.id} (${c.name} v${c.version}, ${c.status})`).join(", ");
      return `run ${runId} is awaiting children that never settled: ${blocking || "none found"}`;
    }
    if (status === "parked")
      return `run ${runId} is parked: no registered flow matches its name and version, or its body drifted — register the flow the run was started with`;
    return `run ${runId} is ${status} with nothing scheduled to wake it`;
  };

  function settle(handle: string): Promise<RunResult<unknown>>;
  function settle<O>(
    handle: RunHandle<O> | string,
    parse: { output: OutputSchema<O> },
  ): Promise<RunResult<O>>;
  async function settle<O>(
    handle: string,
    parse?: { output?: OutputSchema<O> },
  ): Promise<RunResult<unknown>> {
    for (let i = 0; i < MAX_WAKES_PER_SETTLE; i++) {
      await drain();
      const run = await backend.store.loadRunRow(handle);
      if (!run) throw new Error(`settle: run ${handle} not found`);
      if (isTerminal(run.status)) {
        return parse?.output
          ? engine.result(handle, { output: parse.output })
          : engine.result(handle);
      }
      if (run.status === "parked") throw new Error(`settle: ${await stallReason(handle)}`);
      const next = await engine.nextWakeAt();
      if (!next) throw new Error(`settle: ${await stallReason(handle)}`);
      await advanceTo(next);
    }
    throw new Error(
      `settle: run ${handle} did not settle within ${MAX_WAKES_PER_SETTLE} wakes — it may be sleeping in a loop`,
    );
  }

  return {
    engine,

    now: () => at,

    drain,

    advance: (ms) => advanceTo(new Date(at.getTime() + ms)),

    async advanceToNextWake() {
      await drain();
      const next = await engine.nextWakeAt();
      if (!next) return false;
      await advanceTo(next);
      return true;
    },

    settle,
  };
};
