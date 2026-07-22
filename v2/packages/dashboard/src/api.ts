import { type Engine, type FlowEvent, isRunStatus } from "@iterativeflow/core";
import { UI } from "#ui";

/** Dashboard configuration. `events` supplies the durable timeline (e.g. postgres `listEvents`). */
export interface DashboardOpts {
  /** URL prefix the dashboard is mounted under (e.g. "/admin/flows"). Default none. */
  basePath?: string;
  /** Fetch a run's event timeline. Omit if the backend records no events. */
  events?: (runId: string) => Promise<readonly FlowEvent[]>;
}

const json = (body: unknown, status = 200): Response =>
  new Response(JSON.stringify(body), {
    status,
    headers: { "content-type": "application/json" },
  });

const html = (body: string): Response =>
  new Response(body, { headers: { "content-type": "text/html; charset=utf-8" } });

/**
 * A mountable dashboard as a single Web `fetch` handler — works in Node 18+, Bun, Deno, and
 * edge/workers, or behind a thin Express/Hono adapter. Serves a self-contained UI at the base
 * path and a JSON API under `/api` over the {@link Engine} query + control surface.
 */
export const createDashboard = (
  engine: Engine,
  opts: DashboardOpts = {},
): ((req: Request) => Promise<Response>) => {
  const base = opts.basePath ?? "";

  return async (req: Request): Promise<Response> => {
    const url = new URL(req.url);
    const path = url.pathname.startsWith(base)
      ? url.pathname.slice(base.length) || "/"
      : url.pathname;

    if (req.method === "GET" && path === "/") return html(UI);

    if (req.method === "GET" && path === "/api/health") {
      return json(await engine.health());
    }

    if (req.method === "GET" && path === "/api/runs") {
      const statuses = url.searchParams.getAll("status").filter(isRunStatus);
      const filter = {
        status: statuses.length ? statuses : undefined,
        tag: url.searchParams.get("tag") ?? undefined,
        name: url.searchParams.get("name") ?? undefined,
      };
      const page = {
        limit: Math.min(Number(url.searchParams.get("limit") ?? 50), 200),
        cursor: url.searchParams.get("cursor") ?? undefined,
      };
      return json(await engine.listRuns(filter, page));
    }

    const detail = path.match(/^\/api\/runs\/([^/]+)$/);
    if (detail && req.method === "GET") {
      const id = decodeURIComponent(detail[1]);
      const snap = await engine.status(id);
      if (!snap) return json({ error: "run not found" }, 404);
      return json({
        run: snap.run,
        steps: [...snap.steps].map(([cursorKey, outcome]) => ({ cursorKey, ...outcome })),
        signals: snap.signals,
        events: opts.events ? await opts.events(id) : [],
      });
    }

    const cancel = path.match(/^\/api\/runs\/([^/]+)\/cancel$/);
    if (cancel && req.method === "POST") {
      await engine.cancel(decodeURIComponent(cancel[1]));
      return json({ ok: true });
    }

    const retry = path.match(/^\/api\/runs\/([^/]+)\/retry$/);
    if (retry && req.method === "POST") {
      return json({ retried: await engine.retry(decodeURIComponent(retry[1])) });
    }

    const signal = path.match(/^\/api\/runs\/([^/]+)\/signal$/);
    if (signal && req.method === "POST") {
      const body = (await req.json()) as {
        name: string;
        payload?: unknown;
        idempotencyKey?: string;
      };
      const delivered = await engine.signal(
        decodeURIComponent(signal[1]),
        body.name,
        body.payload,
        {
          idempotencyKey: body.idempotencyKey,
        },
      );
      return json({ delivered });
    }

    return json({ error: "not found" }, 404);
  };
};
