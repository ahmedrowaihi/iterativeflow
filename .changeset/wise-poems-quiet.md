---
"iterativeflow": minor
---

Add two round-trip-reduction knobs for connection-constrained / high-throughput deployments (e.g. Aurora Serverless, RDS Proxy):

- `notify?: boolean` (default `true`) — emit Postgres `NOTIFY` on state changes for cross-process `handle.result()` / `handle.wait()` wakeups. Set `false` to drop the `pg_notify` round-trip per step, per signal, and per terminal transition (pair with `results: "poll"` and poll `engine.status()` / use a terminal webhook). The durability-critical `ctx.invoke` parent re-enqueue still runs; only the wakeup NOTIFY is skipped. Also avoids RDS Proxy pinning LISTEN connections.
- `events?: "all" | "lifecycle" | "off"` (default `"all"`) — audit-event granularity in `workflow.events`. `"lifecycle"` drops the high-volume per-step events (`step_started`/`step_ok`/`step_failed`/`step_terminal`), keeping run-level ones; `"off"` records none. `workflow.events` is never read on the resume/claim/reconcile path, so any setting preserves durability and crash-resumability — it only trades observability for fewer DB round-trips (up to two inserts per step).

With `notify: false` + `events: "lifecycle"` (or `"off"`), the per-step write path drops from 5 round-trips to 2 (the `steps` start + finish, which remain the durable source of truth). Defaults are unchanged.
