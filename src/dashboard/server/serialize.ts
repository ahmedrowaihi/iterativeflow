import type { FlowError, RunRow, SignalRow, StepRow, TimerRow } from "../../storage/schema";

/**
 * A JSON value serialized for display. `preview` is pretty-printed JSON,
 * cut at the configured cap; `truncated` says whether anything was cut;
 * `size` is the full serialized length.
 */
export interface CappedJson {
  preview: string;
  truncated: boolean;
  size: number;
}

export const capJson = (value: unknown, cap: number): CappedJson | null => {
  if (value === null || value === undefined) return null;
  let str: string;
  try {
    str = JSON.stringify(value, null, 2) ?? String(value);
  } catch {
    str = String(value);
  }
  return {
    preview: str.length > cap ? str.slice(0, cap) : str,
    truncated: str.length > cap,
    size: str.length,
  };
};

const errorSummary = (error: FlowError | null | undefined) =>
  error ? { code: error.code, message: error.message } : null;

const signalName = (cursorKey: string): string =>
  cursorKey.replace(/^signal:/, "").replace(/:\d+$/, "");

export const toListItem = (row: RunRow) => ({
  id: row.id,
  name: row.name,
  version: row.version,
  status: row.status,
  attempts: row.attempts,
  tags: row.tags,
  parentRunId: row.parentRunId,
  error: errorSummary(row.error),
  createdAt: row.createdAt,
  startedAt: row.startedAt,
  completedAt: row.completedAt,
  updatedAt: row.updatedAt,
});

export const toDetail = (
  run: RunRow,
  steps: StepRow[],
  timers: TimerRow[],
  signals: SignalRow[],
  cap: number,
) => ({
  run: {
    ...toListItem(run),
    parentCursorKey: run.parentCursorKey,
    idempotencyKey: run.idempotencyKey,
    input: capJson(run.input, cap),
    output: capJson(run.output, cap),
    error: run.error ?? null,
  },
  steps: steps.map((s) => ({
    cursorKey: s.cursorKey,
    status: s.status,
    attempts: s.attempts,
    startedAt: s.startedAt,
    completedAt: s.completedAt,
    result: capJson(s.result, cap),
    error: errorSummary(s.error),
  })),
  timers: timers.map((t) => ({
    cursorKey: t.cursorKey,
    fireAt: t.fireAt,
    firedAt: t.firedAt,
  })),
  signals: signals.map((s) => ({
    cursorKey: s.cursorKey,
    name: signalName(s.cursorKey),
    delivered: s.delivered,
    createdAt: s.createdAt,
    deliveredAt: s.deliveredAt,
    expiresAt: s.expiresAt,
    payload: capJson(s.payload, cap),
  })),
});
