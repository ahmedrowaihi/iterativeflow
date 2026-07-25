---
"@iterativeflow/mongodb": minor
---

New backend: **`@iterativeflow/mongodb`** — the four ports over one MongoClient, runs as documents
(one collection per concern, `_id = runId`). The transactional outbox commits across collections in a
multi-document transaction, so a replica set is required (as MongoDB mandates for transactions);
single-document ops like `claim` and `arriveAtJoin` use per-document atomicity. Timestamps are epoch
ms, insertion order is a per-doc ObjectId, and the idempotency/signal-dedup indexes are partial
(`$exists`) so unkeyed docs never collide. Wakeup is in-process. Passes all nine conformance suites.
`mongodb` is a peer dependency.
