---
"iterativeflow": minor
---

Export `isSuspend` and `FlowSuspend` from the public API.

`ctx.sleep` / `ctx.signal` / `ctx.invoke` park a run by throwing `FlowSuspend`. Because it extends `Error`, a `try/catch` around a `ctx.*` call silently swallows the suspend and the run never parks. These were `@internal`, so consumers had no way to guard. The correct pattern is now expressible:

```ts
try {
  await ctx.signal("approval", { timeout: "24h" });
} catch (err) {
  if (isSuspend(err)) throw err; // let the run park
  // ...handle real errors
}
```
