import type { Cursor } from "./cursor";
import type {
  FlowHandle,
  InvokeOpts,
  Logger,
  MetricsRecorder,
  RunSnapshot,
  Storage,
} from "./types";

/**
 * State passed to every lifecycle helper (`runStep`, `runSleep`, `awaitSignal`,
 * `runInvoke`). One per executing run. Lifecycle helpers mutate the snapshot
 * in place when they persist new rows so subsequent `await` points see them.
 *
 * @internal
 */
export interface RunContextState {
  readonly runId: string;
  readonly attempt: number;
  readonly storage: Storage;
  readonly snapshot: RunSnapshot;
  readonly logger: Logger;
  readonly abortSignal: AbortSignal;
  readonly defaultStepTimeoutMs?: number;
  readonly maxStepResultBytes?: number;
  readonly maxInvokeDepth: number;
  readonly maxChildrenPerRun: number;
  readonly metrics?: MetricsRecorder;
  readonly flow?: { name: string; version: number };
  readonly cursor: Cursor;
  readonly startChild?: (
    parentCursorKey: string,
    childHandle: FlowHandle<unknown, unknown>,
    input: unknown,
    opts: InvokeOpts | undefined,
  ) => Promise<string>;
}
