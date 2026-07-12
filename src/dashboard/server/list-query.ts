import type { ListRunsOpts } from "../../engine/types";
import type { RunStatus } from "../../storage/schema";
import { RUN_STATUSES } from "../../storage/schema";

export interface ParsedListQuery {
  opts?: ListRunsOpts;
  error?: string;
}

const MAX_LIST_LIMIT = 500;

export const parseListQuery = (params: URLSearchParams): ParsedListQuery => {
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
