import { type Backend, type IdGen, createLocalWakeup, newId } from "@iterativeflow/core/backend";
import type { ClientSession, MongoClient } from "mongodb";
import { type Names, names } from "#collections";
import { createMongoQueue } from "#queue";
import { createMongoStore } from "#store";
import { createMongoTimer } from "#timer";

export interface MongoBackendOpts {
  /** Database name. Default `iterativeflow`. */
  db?: string;
  /** Collection-name prefix, for running multiple engines in one database. Default none. */
  prefix?: string;
  /** Id generator for runs, signals, and lease tokens. Defaults to {@link newId} (RFC-4122 v4). */
  id?: IdGen;
}

/** @internal */
export const createSessionBackend = (
  client: MongoClient,
  session: ClientSession | undefined,
  opts: MongoBackendOpts = {},
): Backend => {
  const db = client.db(opts.db ?? "iterativeflow");
  const n: Names = names(opts.prefix ?? "");
  const id = opts.id ?? newId;
  return {
    store: createMongoStore(client, db, n, id, session),
    queue: createMongoQueue(db, n, id, session),
    timer: createMongoTimer(db, n, session),
    wakeup: createLocalWakeup(),
  };
};

/**
 * The MongoDB {@link Backend}: the four ports over one `MongoClient`. The outbox commits across
 * collections in one multi-document transaction, so the deployment MUST be a replica set (even a
 * single-node one) — MongoDB requires that for transactions. Wakeup is in-process. Run
 * {@link ensureIndexes} once before use.
 */
export const createMongoBackend = (client: MongoClient, opts: MongoBackendOpts = {}): Backend =>
  createSessionBackend(client, undefined, opts);
