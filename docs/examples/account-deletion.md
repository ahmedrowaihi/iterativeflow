# Account deletion with grace period

User requests deletion → soft-delete + start a 30-day grace flow → on day 30,
hard-delete. User can cancel during grace via `engine.cancel(runId)`.

```ts
const deleteAccount = flow("delete-account")
  .version(1)
  .input(z.object({ userId: z.string(), requestedAt: z.string().datetime() }))

  .step("soft-delete", ({ input }) =>
    db.users.update(input.userId, {
      status: "pending_deletion",
      pendingSince: input.requestedAt,
    }),
  )

  .step("notify-grace", ({ input }) =>
    mail.send({
      idempotencyKey: `grace-notice:${input.userId}`,
      to: input.userId,
      template: "deletion_grace_started",
      data: { graceDays: 30 },
    }),
  )

  .sleep("30d")

  .step("hard-delete", ({ input }) =>
    deletionPipeline.run({
      idempotencyKey: `hard-delete:${input.userId}`,
      userId: input.userId,
    }),
  )

  .step("notify-completed", ({ input }) =>
    mail.send({
      idempotencyKey: `deletion-done:${input.userId}`,
      to: input.userId,
      template: "deletion_completed",
    }),
  )

  .output(({ input }) => ({ userId: input.userId, completed: true }))
  .build();

engine.register(deleteAccount);
const handle = engine.register(deleteAccount);

// User clicks "Delete my account":
const { runId } = await handle.start(
  { userId, requestedAt: new Date().toISOString() },
  // Same user clicking twice → same runId, no second deletion scheduled
  { idempotencyKey: `delete:${userId}` },
);

// Save runId on the user row so the cancel button knows what to undo:
await db.users.update(userId, { deletionRunId: runId });

// User changes their mind during grace period:
await engine.cancel(runId, "user_revoked");
// Then restore: db.users.update(userId, { status: "active", pendingSince: null });
```

Notes:

- `.sleep("30d")` is durable — the run sits in `sleeping` for 30 days; no in-memory timer.
- `idempotencyKey: "delete:${userId}"` on `start` makes "delete" clicked twice return the same `runId`.
- `engine.cancel(runId)` flips to terminal; the day-30 wake job becomes a no-op when the worker sees `canceled`.
- Cancel during an actively-running step is best-effort: the in-flight call finishes, the next replay stops. Design `hard-delete` with external idempotency.
- Changing the grace window → bump `.version(2)`; v1 users in flight keep their original timeline.
