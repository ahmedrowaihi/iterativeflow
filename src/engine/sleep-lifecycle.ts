import { type Duration, toFireAt } from "../util/duration";
import type { RunContextState } from "./context-state";
import { sleepBase } from "./cursor";
import { FlowSuspend } from "./suspend";

/** @internal */
export const runSleep = async (state: RunContextState, duration: Duration): Promise<void> => {
  const cursorKey = state.cursor.next(sleepBase());
  const existing = state.snapshot.timers.get(cursorKey);

  if (existing?.firedAt) return;

  const fireAt = existing?.fireAt ?? toFireAt(duration);
  if (!existing) {
    await state.storage.createTimer(state.runId, cursorKey, fireAt);
    await state.storage.recordEvent({
      runId: state.runId,
      type: "sleep_scheduled",
      cursorKey,
      payload: { fireAt },
    });
  }

  if (fireAt <= new Date()) {
    await state.storage.fireTimer(state.runId, cursorKey);
    await state.storage.recordEvent({
      runId: state.runId,
      type: "sleep_fired",
      cursorKey,
    });
    state.snapshot.timers.set(cursorKey, {
      runId: state.runId,
      cursorKey,
      fireAt,
      firedAt: new Date(),
    });
    return;
  }

  throw new FlowSuspend({ reason: "sleep", wakeAt: fireAt });
};
