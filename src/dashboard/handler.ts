import { readFileSync } from "node:fs";
import { dirname } from "node:path";
import { fileURLToPath } from "node:url";
import type { Engine } from "../engine/engine";
import type { CronSpec, DefaultFlowTables, FlowTables, ListRunsOpts } from "../engine/types";
import type { FlowError, RunRow, RunStatus, SignalRow, StepRow, TimerRow } from "../storage/schema";
import { RUN_STATUSES } from "../storage/schema";
import { locateShippedAsset } from "../util/asset-locate";

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

/**
 * A JSON value serialized for display. `preview` is pretty-printed JSON,
 * cut at the configured cap; `truncated` says whether anything was cut;
 * `size` is the full serialized length.
 */
export interface CappedJson {
  preview: string;
  truncated: boolean;
  size: number;
}

const here = dirname(fileURLToPath(import.meta.url));

const JSON_HEADERS = {
  "content-type": "application/json; charset=utf-8",
  "cache-control": "no-store",
} as const;

const json = (status: number, body: unknown): Response =>
  new Response(JSON.stringify(body), { status, headers: JSON_HEADERS });

const capJson = (value: unknown, cap: number): CappedJson | null => {
  if (value === null || value === undefined) return null;
  let str: string;
  try {
    str = JSON.stringify(value, null, 2) ?? String(value);
  } catch {
    str = String(value);
  }
  return {
    preview: str.length > cap ? str.slice(0, cap) : str,
    truncated: str.length > cap,
    size: str.length,
  };
};

const errorSummary = (error: FlowError | null | undefined) =>
  error ? { code: error.code, message: error.message } : null;

/** List rows drop `input`/`output` (arbitrarily large jsonb) and error stacks. */
const toListItem = (row: RunRow) => ({
  id: row.id,
  name: row.name,
  version: row.version,
  status: row.status,
  attempts: row.attempts,
  tags: row.tags,
  parentRunId: row.parentRunId,
  error: errorSummary(row.error),
  createdAt: row.createdAt,
  startedAt: row.startedAt,
  completedAt: row.completedAt,
  updatedAt: row.updatedAt,
});

const toDetail = (
  run: RunRow,
  steps: StepRow[],
  timers: TimerRow[],
  signals: SignalRow[],
  cap: number,
) => ({
  run: {
    ...toListItem(run),
    parentCursorKey: run.parentCursorKey,
    idempotencyKey: run.idempotencyKey,
    input: capJson(run.input, cap),
    output: capJson(run.output, cap),
    error: run.error ?? null,
  },
  steps: steps.map((s) => ({
    cursorKey: s.cursorKey,
    status: s.status,
    attempts: s.attempts,
    startedAt: s.startedAt,
    completedAt: s.completedAt,
    result: capJson(s.result, cap),
    error: errorSummary(s.error),
  })),
  timers: timers.map((t) => ({
    cursorKey: t.cursorKey,
    fireAt: t.fireAt,
    firedAt: t.firedAt,
  })),
  signals: signals.map((s) => ({
    cursorKey: s.cursorKey,
    delivered: s.delivered,
    createdAt: s.createdAt,
    deliveredAt: s.deliveredAt,
    expiresAt: s.expiresAt,
    payload: capJson(s.payload, cap),
  })),
});

interface ParsedListQuery {
  opts?: ListRunsOpts;
  error?: string;
}

const MAX_LIST_LIMIT = 500;

const parseListQuery = (params: URLSearchParams): ParsedListQuery => {
  const opts: ListRunsOpts & {
    status?: RunStatus[];
    cursor?: { createdAt: Date; id: string };
  } = {};

  const name = params.get("name");
  if (name) opts.name = name;

  const tag = params.get("tag");
  if (tag) opts.tag = tag;

  const status = params.get("status");
  if (status) {
    const statuses: RunStatus[] = [];
    for (const raw of status.split(",")) {
      const candidate = raw.trim();
      if (!candidate) continue;
      if (!(RUN_STATUSES as readonly string[]).includes(candidate)) {
        return { error: `invalid status: ${candidate}` };
      }
      statuses.push(candidate as RunStatus);
    }
    if (statuses.length > 0) opts.status = statuses;
  }

  for (const key of ["since", "until"] as const) {
    const raw = params.get(key);
    if (!raw) continue;
    const date = new Date(raw);
    if (Number.isNaN(date.getTime())) return { error: `invalid ${key}: ${raw}` };
    opts[key] = date;
  }

  const limit = params.get("limit");
  if (limit) {
    const parsed = Number(limit);
    if (!Number.isInteger(parsed) || parsed < 1 || parsed > MAX_LIST_LIMIT) {
      return { error: `invalid limit: ${limit} (1..${MAX_LIST_LIMIT})` };
    }
    opts.limit = parsed;
  }

  const cursorCreatedAt = params.get("cursorCreatedAt");
  const cursorId = params.get("cursorId");
  if ((cursorCreatedAt === null) !== (cursorId === null)) {
    return { error: "cursorCreatedAt and cursorId must be provided together" };
  }
  if (cursorCreatedAt !== null && cursorId !== null) {
    const createdAt = new Date(cursorCreatedAt);
    if (Number.isNaN(createdAt.getTime())) {
      return { error: `invalid cursorCreatedAt: ${cursorCreatedAt}` };
    }
    opts.cursor = { createdAt, id: cursorId };
  }

  return { opts };
};

const isJsonRequest = (req: Request): boolean =>
  (req.headers.get("content-type") ?? "").toLowerCase().startsWith("application/json");

/**
 * Create a mountable flows dashboard for an engine. Consumes only the public
 * Engine API (`listRuns`, `status`, `health`, `cancel`, `retry`) — no direct
 * table access — so it works against any deployment the engine works against.
 * If the host also passes `crons`, the dashboard lists them and can trigger
 * one on demand; those specs are host-supplied, not read from `engine`.
 */
export const createFlowsDashboard = <T extends FlowTables = DefaultFlowTables>(
  opts: FlowsDashboardOpts<T>,
): FlowsDashboard => {
  const { engine } = opts;
  const cap = opts.jsonCap ?? 20_000;
  const crons = opts.crons ?? [];

  let htmlSource: string | undefined;
  const loadHtml = (): string => {
    htmlSource ??= readFileSync(locateShippedAsset(here, "dashboard.html", "index.html"), "utf8");
    return htmlSource;
  };

  // The UI uses relative URLs; a <base> tag pinned to the request path makes
  // them resolve under whatever path the handler is mounted at.
  const htmlResponse = (pathname: string): Response => {
    const basePath = pathname.endsWith("/") ? pathname : `${pathname}/`;
    const body = loadHtml().replace('<base href="./">', `<base href="${basePath}">`);
    return new Response(body, {
      status: 200,
      headers: { "content-type": "text/html; charset=utf-8", "cache-control": "no-store" },
    });
  };

  const getRuns = async (params: URLSearchParams): Promise<Response> => {
    const { opts: listOpts, error } = parseListQuery(params);
    if (error) return json(400, { error });
    const page = await engine.listRuns(listOpts);
    return json(200, {
      runs: (page.runs as unknown as RunRow[]).map(toListItem),
      next: page.next ?? null,
    });
  };

  const getRun = async (runId: string): Promise<Response> => {
    const detail = await engine.status(runId);
    if (!detail) return json(404, { error: "run not found" });
    return json(
      200,
      toDetail(
        detail.run as unknown as RunRow,
        detail.steps as unknown as StepRow[],
        detail.timers as unknown as TimerRow[],
        detail.signals as unknown as SignalRow[],
        cap,
      ),
    );
  };

  const postCancel = async (req: Request, runId: string): Promise<Response> => {
    let reason: string | undefined;
    try {
      const body = (await req.json()) as { reason?: unknown };
      if (typeof body?.reason === "string" && body.reason.length > 0) reason = body.reason;
    } catch {
      // empty or malformed body — cancel without a reason
    }
    await engine.cancel(runId, reason);
    return json(200, { ok: true });
  };

  const postRetry = async (runId: string): Promise<Response> => {
    const result = await engine.retry(runId);
    switch (result.kind) {
      case "queued":
        return json(200, result);
      case "missing":
        return json(404, { ...result, error: "run not found" });
      case "not_failed":
        return json(409, { ...result, error: `run is ${result.status}, only failed runs retry` });
    }
  };

  const getCrons = (): Response =>
    json(200, {
      crons: crons.map((c) => ({
        name: c.name,
        schedule: c.schedule,
        timezone: c.timezone ?? "UTC",
        overlap: c.overlap ?? "skip",
        jitterMs: c.jitterMs ?? 0,
        backfillPeriod: c.backfillPeriod ?? 0,
      })),
    });

  const postCronRun = async (name: string): Promise<Response> => {
    const spec = crons.find((c) => c.name === name);
    if (!spec) return json(404, { error: "cron not found" });
    const result = await spec.run();
    return json(200, { ok: true, result: capJson(result, cap) });
  };

  const handleApi = async (req: Request, segments: string[]): Promise<Response> => {
    const method = req.method.toUpperCase();

    if (segments.length === 1 && segments[0] === "health") {
      if (method !== "GET") return json(405, { error: "method not allowed" });
      return json(200, await engine.health());
    }

    if (segments[0] === "runs") {
      if (segments.length === 1) {
        if (method !== "GET") return json(405, { error: "method not allowed" });
        return getRuns(new URL(req.url).searchParams);
      }
      const runId = segments[1];
      if (segments.length === 2) {
        if (method !== "GET") return json(405, { error: "method not allowed" });
        return getRun(runId);
      }
      if (segments.length === 3 && (segments[2] === "cancel" || segments[2] === "retry")) {
        if (method !== "POST") return json(405, { error: "method not allowed" });
        // Mutations require a JSON content type: HTML forms can't send one
        // cross-origin without a CORS preflight, which closes off CSRF when
        // the dashboard is mounted behind cookie auth.
        if (!isJsonRequest(req)) {
          return json(415, { error: "content-type must be application/json" });
        }
        return segments[2] === "cancel" ? postCancel(req, runId) : postRetry(runId);
      }
    }

    if (segments[0] === "crons") {
      if (segments.length === 1) {
        if (method !== "GET") return json(405, { error: "method not allowed" });
        return getCrons();
      }
      if (segments.length === 3 && segments[2] === "run") {
        if (method !== "POST") return json(405, { error: "method not allowed" });
        if (!isJsonRequest(req)) {
          return json(415, { error: "content-type must be application/json" });
        }
        return postCronRun(segments[1]);
      }
    }

    return json(404, { error: "not found" });
  };

  const fetchHandler = async (req: Request): Promise<Response> => {
    const url = new URL(req.url);
    const apiIdx = url.pathname.lastIndexOf("/api/");

    if (apiIdx !== -1) {
      const segments = url.pathname
        .slice(apiIdx + "/api/".length)
        .split("/")
        .filter((s) => s.length > 0)
        .map(decodeURIComponent);
      if (segments.length > 0) {
        try {
          return await handleApi(req, segments);
        } catch (err) {
          return json(500, { error: err instanceof Error ? err.message : String(err) });
        }
      }
    }

    // Anything that isn't an API route serves the app, so the handler works
    // at any mount path (including ones that contain `/api/` themselves).
    if (req.method.toUpperCase() !== "GET") return json(405, { error: "method not allowed" });
    return htmlResponse(url.pathname);
  };

  return { fetch: fetchHandler };
};
