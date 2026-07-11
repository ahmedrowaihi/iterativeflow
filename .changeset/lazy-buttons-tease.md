---
"iterativeflow": minor
---

Add `iterativeflow/dashboard` — a mountable operations console for your flows.
Mount one fetch handler behind your own auth and browse everything the engine
knows, with no extra services and no runtime dependencies added to your app.

![Runs list with filters and pagination](docs/assets/dashboard-runs.png)

- **Runs** — browse, filter (flow, status, tag, date range), and page through
  every run.
- **Run detail** — steps, sleeps, signals, and capped input/output payloads in a
  slide-over sheet, with highlighted JSON and copy buttons.
- **Act on a run** — cancel, retry, or deliver a signal (with a JSON payload) to
  a run that's waiting on one.
- **Crons** — trigger a cron on demand and see the runs it started.
- **Overview** — status distribution, recent runs, and active crons at a glance.
- **Shareable & refresh-safe** — the open run/cron sheet and active filters live
  in the URL, so a reload or a shared link restores exactly what you were looking
  at.

```ts
import { createFlowsDashboard } from "iterativeflow/dashboard";

const dashboard = createFlowsDashboard({ engine, crons });
// dashboard.fetch: (req: Request) => Promise<Response> — mount it behind your auth
```

Pass `crons` for the crons view, `jsonCap` to bound payload previews, and
`theme` to match your app.

![Run detail — steps, error, payloads](docs/assets/dashboard-run-detail.png)
![A cron and the runs it started](docs/assets/dashboard-crons.png)

Thanks [@ramisalem](https://github.com/ramisalem) for kicking this off in #14.
