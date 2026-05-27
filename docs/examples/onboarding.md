# User onboarding drip + survey

Welcome email → wait 3 days → tips email → wait 11 days → survey hook with
14-day timeout → finalize.

```ts
const onboarding = flow("onboarding")
  .version(1)
  .input(z.object({ userId: z.string(), email: z.string().email() }))

  .step("welcome-email", ({ input }) =>
    mail.send({
      idempotencyKey: `welcome:${input.userId}`,
      to: input.email,
      template: "welcome",
    }),
  )

  .sleep("3d")

  .step("tips-email", ({ input }) =>
    mail.send({
      idempotencyKey: `tips:${input.userId}`,
      to: input.email,
      template: "tips",
    }),
  )

  .sleep("11d")

  .hook(
    "survey",
    {
      schema: z.object({ score: z.number().min(0).max(10) }),
      timeout: "14d",
    },
    (input: { userId: string; email: string }, payload) => ({
      ...input,
      score: payload.score,
    }),
  )

  .step("finalize", ({ input }) =>
    analytics.track({
      userId: input.userId,
      event: "onboarding_completed",
      properties: { nps: input.score },
    }),
  )

  .output(({ input }) => ({ userId: input.userId, nps: input.score }))
  .build();

const handle = engine.register(onboarding);
await handle.start({ userId: "u_1", email: "a@b.co" });

// from your survey webhook:
await engine.signal(runId, "survey", { score: 9 });
```

Notes:

- The `.sleep`s are durable — the run lives in `sleeping` status; no in-memory timer.
- Hook `merge` folds the prior `input` (userId/email) with the new payload (`score`) so `finalize` sees both. Without `merge`, the channel would become just `{ score }`.
- User never responds → `WORKFLOW_HOOK_TIMEOUT` at day 28. To change the cadence: bump `.version(2)`. Editing `.sleep("3d")` in place trips the compat guard on resume.
