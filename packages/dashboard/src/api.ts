import { type Engine, type FlowEvent, isRunStatus } from "@iterativeflow/core";
import { renderUI } from "#ui";

/** Dashboard configuration. `events` supplies the durable timeline (e.g. postgres `listEvents`). */
export interface DashboardOpts {
  /** URL prefix the dashboard is mounted under (e.g. "/admin/flows"). Default none. */
  basePath?: string;
  /** Fetch a run's event timeline. Omit if the backend records no events. */
  events?: (runId: string) => Promise<readonly FlowEvent[]>;
}

const json = <T>(body: T, status = 200): Response =>
  new Response(JSON.stringify(body), {
    status,
    headers: { "content-type": "application/json" },
  });

const html = (body: string): Response =>
  new Response(body, { headers: { "content-type": "text/html; charset=utf-8" } });

// Clamp rather than reject: a hostile or malformed `?limit` gets the default page, not a 500.
const pageLimit = (raw: string | null): number => {
  const n = Number(raw ?? 50);
  return Number.isInteger(n) && n > 0 ? Math.min(n, 200) : 50;
};

/**
 * A mountable dashboard as a single Web `fetch` handler — works in Node 18+, Bun, Deno, and
 * edge/workers, or behind a thin Express/Hono adapter. Serves a self-contained UI at the base
 * path and a JSON API under `/api` over the {@link Engine} query + control surface.
 *
 * The mutating routes (cancel, retry, signal) are UNAUTHENTICATED: mount this behind your own
 * admin auth, and add CSRF protection — they take no custom header.
 */
export const createDashboard = (
  engine: Engine,
  opts: DashboardOpts = {},
): ((req: Request) => Promise<Response>) => {
  const base = (opts.basePath ?? "").replace(/\/+$/, "");
  const page = renderUI(base);

  return async (req: Request): Promise<Response> => {
    const url = new URL(req.url);
    const inside = url.pathname === base || url.pathname.startsWith(`${base}/`);
    if (!inside) return json({ error: "not found" }, 404);
    const path = url.pathname.slice(base.length) || "/";

    if (req.method === "GET" && path === "/") return html(page);

    if (req.method === "GET" && path === "/api/health") {
      return json(await engine.health());
    }

    if (req.method === "GET" && path === "/api/metrics") {
      const names = url.searchParams.getAll("name");
      return json({ pendingWork: await engine.pendingWork(names.length ? names : undefined) });
    }

    if (req.method === "GET" && path === "/api/runs") {
      const statuses = url.searchParams.getAll("status").filter(isRunStatus);
      const filter = {
        status: statuses.length ? statuses : undefined,
        tag: url.searchParams.get("tag") ?? undefined,
        name: url.searchParams.get("name") ?? undefined,
      };
      return json(
        await engine.listRuns(filter, {
          limit: pageLimit(url.searchParams.get("limit")),
          cursor: url.searchParams.get("cursor") ?? undefined,
        }),
      );
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
        children: (await engine.children(id)).map((c) => ({
          id: c.id,
          name: c.name,
          version: c.version,
          status: c.status,
        })),
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

    const signal = path.match(/^\/api\/runs\/([^/]+)\/signals\/([^/]+)$/);
    if (signal && req.method === "POST") {
      const text = await req.text();
      let payload;
      try {
        payload = text === "" ? undefined : JSON.parse(text);
      } catch {
        return json({ error: "signal payload must be JSON" }, 400);
      }
      const delivered = await engine.signal(
        decodeURIComponent(signal[1]),
        decodeURIComponent(signal[2]),
        payload,
        { idempotencyKey: req.headers.get("idempotency-key") ?? undefined },
      );
      return json({ delivered });
    }

    return json({ error: "not found" }, 404);
  };
};
