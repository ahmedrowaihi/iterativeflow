---
"@iterativeflow/core": minor
---

Child flows:

- **`ctx.invoke(…, { onChildFailure: "settle" })`** waits for every child and returns each one's
  result (`{ status: "done", output }`, `{ status: "failed", error }` or `{ status: "canceled" }`)
  instead of failing the parent on the first failure. Works for one child and for a fan-out.
- **Canceling a child now wakes its parent.** A parent waiting on a child that was canceled directly
  used to stay parked until the next reconcile sweep.
- **The dashboard run view lists a run's children and links a child to its parent.**
- **`settle` in the test harness names the child a stuck parent is waiting on**, with its status.
