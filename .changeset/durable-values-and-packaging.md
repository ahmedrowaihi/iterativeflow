---
"@iterativeflow/core": patch
---

Make the memo's runtime type the same on every backend, and gate the published packages.

`ctx.step` is typed `Promise<T>`, but the memo round-trips through the backend's storage, so what a
replay hands back was **backend-dependent**: Postgres/MySQL/SQLite/Redis/DynamoDB store JSON and turn
a `Date` into a string, MongoDB stores BSON and kept it a `Date`, and the in-memory backend used
`structuredClone` and kept it too. So `ctx.step("now", () => new Date())` returned a `Date` in tests
and a `string` in production — the divergence was invisible precisely where you'd catch it.

Values that cross the durable boundary (run input/output/error, step results, signal payloads) are
now JSON-normalized on MongoDB and in memory, matching what every other backend already did. A new
store-conformance case pins it, and it is what caught the MongoDB divergence — the doc change alone
had asserted "a string on every backend", which was false.

`ctx.step`'s JSDoc now says this outright: `T` describes what `fn` returns, not necessarily what a
replay hands back.

Also adds a packaging gate (`publint` over all 12 packages in CI — every `exports`, `files` and
`types` entry must point at something the build actually emits; all 12 pass today, so this locks in
a property that was previously unverified luck), a Renovate config that groups non-major updates and
keeps backend drivers and `tsdown` on their own PRs, and a note in the deployment guide that
`applySchema` builds indexes and a plain `CREATE INDEX` takes a write lock — at scale, run it as a
migration or pre-create with `CONCURRENTLY`.
