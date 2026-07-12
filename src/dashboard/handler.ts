import { dirname } from "node:path";
import { fileURLToPath } from "node:url";
import type { Engine } from "../engine/engine";
import type { CronSpec, DefaultFlowTables, FlowTables } from "../engine/types";
import { createApiRouter } from "./server/api";
import { createAssetServer, readDashboardUi } from "./server/assets";
import { json } from "./server/http";
import { renderTheme } from "./server/theme";
import type { DashboardTheme } from "./server/theme";

export type { CappedJson } from "./server/serialize";
export type { DashboardTheme, ThemeToken, ThemeTokens } from "./server/theme";

/** Options for {@link createFlowsDashboard}. */
export interface FlowsDashboardOpts<T extends FlowTables = DefaultFlowTables> {
  /**
   * The engine whose runs the dashboard shows. A `createEngine(...)` instance
   * is inert until `listen()` — passing one here never starts a worker, so an
   * API/web process can host the dashboard without consuming the queue.
   */
  engine: Engine<T>;
  /**
   * Max serialized characters for any JSON payload (run input/output, step
   * results, signal payloads) sent to the browser. Larger values are cut to a
   * preview and flagged `truncated`. Default 20 000.
   */
  jsonCap?: number;
  /**
   * Cron specs the host has registered with the engine — the same objects
   * passed to `engine.defineCron(...)` — so the dashboard can list them and
   * trigger one on demand. The engine has no public API to enumerate its own
   * crons, so these are supplied directly rather than read back from
   * `engine`. Triggering here calls `run()` directly: it bypasses the
   * engine's own overlap lock, so a manual run can run alongside a scheduled
   * one even when `overlap: "skip"` is set. Omit to hide the crons panel.
   */
  crons?: CronSpec[];
  /**
   * Override the built-in theme so the dashboard matches your app. The UI is
   * styled with the shadcn/ui token contract plus a `--status-*` run-status
   * palette, and toggles a `.dark` class from the OS color scheme. Supply the
   * same token values your app uses for `light` / `dark`; the generated CSS is
   * injected into `<head>` after the built-in stylesheet, so it wins.
   */
  theme?: DashboardTheme;
}

/**
 * A mounted dashboard. `fetch` is a WHATWG `Request => Response` handler:
 * wire it to a Next.js route handler, `app.mount(...)` in Hono, or any
 * framework that speaks fetch. The handler serves both the JSON API (paths
 * containing `/api/`) and the single-page UI (any other GET).
 *
 * The dashboard performs NO authentication — mount it behind your own.
 */
export interface FlowsDashboard {
  fetch: (req: Request) => Promise<Response>;
}

const API_ROOTS = new Set(["health", "runs", "crons"]);

/**
 * Create a mountable flows dashboard for an engine. Consumes only the public
 * Engine API (`listRuns`, `status`, `health`, `cancel`, `retry`, `signal`) —
 * no direct table access — so it works against any deployment the engine works
 * against. If the host also passes `crons`, the dashboard lists them and can
 * trigger one on demand; those specs are host-supplied, not read from `engine`.
 */
export const createFlowsDashboard = <T extends FlowTables = DefaultFlowTables>(
  opts: FlowsDashboardOpts<T>,
): FlowsDashboard => {
  const cap = opts.jsonCap ?? 20_000;
  const crons = opts.crons ?? [];

  const here = dirname(fileURLToPath(import.meta.url));
  const ui = readDashboardUi(here);
  const { assetResponse, htmlResponse } = createAssetServer(ui, renderTheme(opts.theme));
  const handleApi = createApiRouter(opts.engine, crons, cap);

  const fetchHandler = async (req: Request): Promise<Response> => {
    const url = new URL(req.url);
    const apiIdx = url.pathname.lastIndexOf("/api/");

    if (apiIdx !== -1) {
      let segments: string[];
      try {
        segments = url.pathname
          .slice(apiIdx + "/api/".length)
          .split("/")
          .filter((s) => s.length > 0)
          .map(decodeURIComponent);
      } catch {
        return json(400, { error: "malformed url encoding" });
      }
      if (segments.length > 0 && API_ROOTS.has(segments[0])) {
        try {
          return await handleApi(req, segments);
        } catch (err) {
          return json(500, { error: err instanceof Error ? err.message : String(err) });
        }
      }
    }

    if (req.method.toUpperCase() !== "GET") return json(405, { error: "method not allowed" });
    const name = url.pathname.slice(url.pathname.lastIndexOf("/") + 1);
    return assetResponse(name) ?? htmlResponse(url.pathname);
  };

  return { fetch: fetchHandler };
};
