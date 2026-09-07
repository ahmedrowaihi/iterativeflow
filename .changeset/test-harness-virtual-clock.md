---
"@iterativeflow/core": minor
---

New: `@iterativeflow/core/testing` — a virtual clock over the real engine, so a flow that sleeps for
three days settles in a millisecond.

Testing a durable flow meant hand-rolling a `Clock`, pumping `tickOnce` in a loop, and guessing how
far to advance between ticks. `createTestHarness(backend, flows)` does that properly:

```ts
const t = createTestHarness(createMemoryBackend(), [onboard]);
const handle = await t.engine.submit(onboard, { userId: "u_1" });
await t.advanceToNextWake(); // runs to the sleep, then jumps it
await t.engine.signal(handle, "survey", { score: 9 });
expect(await t.settle(handle)).toMatchObject({ status: "done" });
```

- `settle(handle)` drives a run to its terminal outcome, jumping every sleep and retry backoff. When
  the run parks on something nothing will deliver, it throws naming what it waited on — a missing
  `engine.signal(...)` in the test is the usual cause — instead of hanging until the test times out.
- `advanceToNextWake()` drains first, then jumps to the earliest pending deadline, so a freshly
  submitted run reaches its sleep before the clock is asked where to jump. `false` means nothing is
  scheduled.
- `advance(ms)` and `drain()` for finer control; `now()` reads the virtual instant.

It is a third entry point on `core` rather than a new package, and it takes any `Backend` — the
in-memory one for unit tests, or a real database for an integration test that still doesn't wait out
a sleep. Both loops are bounded and throw a diagnostic rather than spinning.

Also fixes a latent snapshot-stability bug in `scripts/api-snapshot.mjs`: it normalized the content
hash of the `id-<hash>` chunk only. A third entry makes tsdown emit further shared chunks, whose
un-normalized hashes would have churned `etc/core.api.md` — and failed CI's `git diff --exit-code
etc` — on any private edit to core. Every chunk hash is now neutralized, in both filenames and import
specifiers.
