---
"iterativeflow": major
---

## v1.0 — durable, iterative workflows on your own Postgres

```ts
const onboard = flow("onboard")
  .input(z.object({ userId: z.string() }))
  .step("create-account", ({ input }) => createAccount(input.userId))
  .sleep("3d")
  .hook("survey", { schema: z.object({ score: z.number() }) })
  .output(({ input }) => ({ score: input.score }))
  .build();

const handle = engine.register(onboard);
const { runId } = await handle.start({ userId: "u_1" });

// 3 days later, from a webhook:
await engine.signal(runId, "survey", { score: 9 });
```

The run lives in Postgres for three days. Workers can crash, deploys can roll, the process can be killed and restarted — when the timer fires, the workflow resumes from where it left off.

Inspired by [Trigger.dev's workflow SDK](https://trigger.dev) and [Temporal](https://temporal.io). Runs inside your Node app on top of [graphile-worker](https://worker.graphile.org) and [drizzle-orm](https://orm.drizzle.team). No extra service to host.

### Engine

- **Builder** — `flow().step().sleep().hook().loop().output().build()` with a single value channel and typed I/O
- **`engine.defineWorkflow`** — raw escape hatch for dynamic graphs and infinite loops
- **Versioned flows** — `INCOMPATIBLE_VERSION` / `NON_DETERMINISTIC` on graph drift; never silent corruption
- **Transactional outbox** — state writes + queue insert commit atomically; reconciler re-enqueues orphans
- **Lock-order rule** — `runs FOR UPDATE` first everywhere; no deadlock by construction
- **Per-step `timeoutMs`** so a hung function can't pin a worker forever
- **Retention** — `engine.pruneEvents` / `engine.pruneRuns`

### Compatibility

- **[Standard Schema](https://standardschema.dev)** for validation — bring your own (zod, valibot, arktype, …)
- **Zero runtime dependency** on zod or ms
- Works with stable **drizzle 0.45+** and **graphile-worker 0.16+** (also the v1-rc lines)

### Dev + release pipeline

- **Pre-commit / pre-push hooks** via [lefthook](https://lefthook.dev) — lint, format, typecheck, tests
- **Oxc tooling** — [oxlint](https://oxc.rs) + [oxfmt](https://oxc.rs) for fast lint + format
- **PR previews** via [pkg.pr.new](https://pkg.pr.new) — every PR gets an installable build
- **OIDC trusted publishing** to npm — no `NPM_TOKEN`, no token rotation; releases driven entirely by merging the changesets PR

Full guide: [`docs/guide.md`](./docs/guide.md). Worked examples (checkout, onboarding, multi-agent + human-in-loop, multi-signer, saga, account deletion): [`docs/examples/`](./docs/examples/).
