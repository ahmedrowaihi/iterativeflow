import { type Backend, type IdGen, createLocalWakeup, newId } from "@iterativeflow/core/backend";
import type { Doc } from "#client";
import { createDynamoQueue } from "#queue";
import { DEFAULT_TABLE } from "#schema";
import { createDynamoStore } from "#store";
import { createDynamoTimer } from "#timer";

export interface DynamoBackendOpts {
  /** The single table all four ports share. Default {@link DEFAULT_TABLE}. */
  table?: string;
  /** Id generator for runs, signals, and lease tokens. Defaults to {@link newId} (RFC-4122 v4). */
  id?: IdGen;
}

/**
 * The DynamoDB {@link Backend}: the four ports over one document client and one table. Store,
 * Queue, and Timer share that table, so an outbox commits as one `TransactWriteItems` — the
 * single transactional domain the seam requires. Pass a client from {@link docClient}, or any
 * object exposing `send`.
 */
export const createDynamoBackend = (doc: Doc, opts: DynamoBackendOpts = {}): Backend => {
  const table = opts.table ?? DEFAULT_TABLE;
  const id = opts.id ?? newId;
  return {
    store: createDynamoStore(doc, table, id),
    queue: createDynamoQueue(doc, table, id),
    timer: createDynamoTimer(doc, table),
    wakeup: createLocalWakeup(),
  };
};
