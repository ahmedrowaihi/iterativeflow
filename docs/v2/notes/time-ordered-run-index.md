# Proposal: a time-ordered run index (`gsi2` sort key)

- **Status:** Proposed (not built). Field report from the DynamoDB backend driving a Lambda +
  EventBridge deployment — the run list came back in effectively arbitrary order across instances.
- **Scope:** `@iterativeflow/dynamodb` — make the `gsi2` sort key derive from a time source instead of
  the in-process `nextSeq` counter, so `listRuns` returns runs in a stable global order. Additive;
  no API change.

## Why

`listRuns` orders by `gsi2` (`gsi2pk = "RUN"`, range = the seq). The seq is:

```ts
// codec.ts
let counter = 0;
export const nextSeq = (): number => ++counter; // "would source it from a Dynamo atomic counter"
```

`counter` is **module-level, in-process**. On a serverless deployment every Lambda cold start resets
it to `0`, so runs created in different instances get **colliding / rewound** sequence numbers. Two
runs submitted seconds apart from two warm instances can sort in either order — or identically. The
result: `listRuns` (and any UI reading it) shows runs in an order that has nothing to do with when
they were created. Our ops panel had to re-sort each page by `createdAt` client-side to compensate,
which only fixes ordering *within* a page — cross-page order is still wrong.

The comment already flags the counter as a placeholder. The cheapest correct source is the one the
row already carries: **`createdAt`**.

## Shape

The run row already stores `createdAt` (an ISO string; the backend defaults it to its own clock when a
direct `startRun` omits it). Make the `gsi2` sort key a **lexicographically-sortable timestamp** plus a
disambiguator, instead of `pad(seq)`:

```ts
// gsi2sk = <epoch-ms, zero-padded><runId suffix>   — monotonic across instances, unique per run
const gsi2sk = `${pad(createdAt.getTime())}#${runId}`;
```

- **Global order** — every writer uses the same wall clock, so newest-first is correct across
  instances without any shared counter.
- **Ties** — same-millisecond submits disambiguate by `runId`; stable, never colliding.
- **`listRuns`** — unchanged query (`gsi2pk = "RUN"`, `ScanIndexForward: false`), now genuinely
  newest-first.

`nextSeq` can stay for anywhere an intra-process monotonic tiebreak is still wanted (e.g. signal
ordering within one run), but it must not be the cross-run sort key.

## Notes / open questions

- **Clock skew** between writers can reorder near-simultaneous runs by a few ms. That's acceptable for a
  "newest-first" list (and strictly better than the current reset-to-zero); a strict total order would
  need a Dynamo atomic counter (`UpdateItem ADD`), one extra write per submit — probably not worth it
  just for list ordering.
- **Backfill** — existing rows keep their old `pad(seq)` sort key; a one-off migration could rewrite
  `gsi2sk` from `createdAt`, or the mixed ordering just ages out. New rows are correct immediately.
- **Backwards compatible** — the index name, `gsi2pk`, and the query are unchanged; only the sort-key
  *encoding* changes. `Page.cursor` remains an opaque token.
