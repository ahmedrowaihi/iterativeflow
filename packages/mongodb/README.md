# @iterativeflow/mongodb

A MongoDB backend for [iterativeflow](https://github.com/ahmedrowaihi/iterativeflow) — the four ports
(Store / Queue / Timer / Wakeup) over one MongoClient. The transactional outbox commits across
collections in a multi-document transaction, so the deployment must be a **replica set** (even a
single-node one — MongoDB requires that for transactions). Passes the same conformance suites as the
SQL and Redis backends.

```ts
import { MongoClient } from "mongodb";
import { createMongoBackend, ensureIndexes } from "@iterativeflow/mongodb";
import { createEngine } from "@iterativeflow/core";

const client = new MongoClient(process.env.MONGO_URL); // a replica-set connection string
await client.connect();
await ensureIndexes(client.db("iterativeflow"));
const engine = createEngine(createMongoBackend(client), [
  /* your flows */
]);
```

## Notes

- **`mongodb` is a peer dependency.** Requires a replica set for transactions.
- **Wakeup is in-process** (like the SQL/Redis backends).
- Runs are documents keyed by `_id = runId`; steps/signals/jobs/timers/crons are their own
  collections. Cross-collection writers (the outbox) use a session transaction; single-document ops
  (`claim`, `arriveAtJoin`, …) use Mongo's per-document atomicity, no transaction needed.
- Run `ensureIndexes(db, prefix?)` once — the idempotency and signal-dedup indexes are **partial**
  (`idempotency_key` / `idem_key` `$exists`), so unkeyed docs never collide.
