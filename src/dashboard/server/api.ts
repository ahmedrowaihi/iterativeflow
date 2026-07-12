import type { Engine } from "../../engine/engine";
import type { CronSpec, FlowTables } from "../../engine/types";
import type { RunRow, SignalRow, StepRow, TimerRow } from "../../storage/schema";
import { isJsonRequest, json } from "./http";
import { parseListQuery } from "./list-query";
import { capJson, toDetail, toListItem } from "./serialize";

export type ApiRouter = (req: Request, segments: string[]) => Promise<Response>;

export const createApiRouter = <T extends FlowTables>(
  engine: Engine<T>,
  crons: CronSpec[],
  cap: number,
): ApiRouter => {
  const getRuns = async (params: URLSearchParams): Promise<Response> => {
    const { opts, error } = parseListQuery(params);
    if (error) return json(400, { error });
    const page = await engine.listRuns(opts);
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

  const postSignal = async (req: Request, runId: string): Promise<Response> => {
    let name: string;
    let payload: unknown;
    try {
      const body = (await req.json()) as { name?: unknown; payload?: unknown };
      if (typeof body?.name !== "string" || body.name.length === 0) {
        return json(400, { error: "signal name required" });
      }
      name = body.name;
      payload = body.payload;
    } catch {
      return json(400, { error: "invalid JSON body" });
    }
    const result = await engine.signal(runId, name, payload);
    switch (result.kind) {
      case "invalid_payload":
        return json(422, { ...result, error: "signal payload rejected" });
      case "expired":
        return json(409, { ...result, error: "signal window expired" });
      default:
        return json(200, result);
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

  return async (req, segments) => {
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
      if (
        segments.length === 3 &&
        (segments[2] === "cancel" || segments[2] === "retry" || segments[2] === "signal")
      ) {
        if (method !== "POST") return json(405, { error: "method not allowed" });
        // Mutations require a JSON content type: HTML forms can't send one
        // cross-origin without a CORS preflight, which closes off CSRF when
        // the dashboard is mounted behind cookie auth.
        if (!isJsonRequest(req)) {
          return json(415, { error: "content-type must be application/json" });
        }
        if (segments[2] === "cancel") return postCancel(req, runId);
        if (segments[2] === "retry") return postRetry(runId);
        return postSignal(req, runId);
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
};
