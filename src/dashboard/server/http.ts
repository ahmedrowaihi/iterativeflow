const JSON_HEADERS = {
  "content-type": "application/json; charset=utf-8",
  "cache-control": "no-store",
} as const;

export const json = (status: number, body: unknown): Response =>
  new Response(JSON.stringify(body), { status, headers: JSON_HEADERS });

export const isJsonRequest = (req: Request): boolean =>
  (req.headers.get("content-type") ?? "").toLowerCase().startsWith("application/json");
