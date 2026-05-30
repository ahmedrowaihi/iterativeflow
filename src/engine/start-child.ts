import type { FlowHandle, InvokeOpts, Storage } from "./types";

/**
 * Create-or-skip a child run keyed by `(parentRunId, parentCursorKey)`. If a
 * row already exists for that pair (resumed replay), returns its `runId`
 * without enqueuing. Otherwise inserts, records the started event, and
 * enqueues for the worker.
 *
 * @internal
 */
export const createStartChild =
  (storage: Storage) =>
  async (
    parentRunId: string,
    parentCursorKey: string,
    childHandle: FlowHandle<unknown, unknown>,
    input: unknown,
    invokeOpts: InvokeOpts | undefined,
  ): Promise<string> => {
    const idemSeed = `${parentRunId}:${parentCursorKey}`;
    return storage.transaction(async (tx) => {
      const { runId, created } = await tx.createRun({
        name: childHandle.name,
        version: childHandle.version,
        input,
        idempotencyKey: invokeOpts?.idempotencyKey ?? idemSeed,
        tags: invokeOpts?.tags,
        parentRunId,
        parentCursorKey,
      });
      if (created) {
        await tx.recordEvent({
          runId,
          type: "started",
          payload: { parent: parentRunId, parentCursorKey },
        });
        await tx.enqueue(runId);
      }
      return runId;
    });
  };
