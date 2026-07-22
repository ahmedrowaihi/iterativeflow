import type { EventSink, EventType, FlowEvent } from "@iterativeflow/core/backend";
import { j } from "#codec";
import { tables } from "#schema";
import type { Sql } from "#sql";

/**
 * Durable Postgres {@link EventSink} — the dashboard timeline. Off by default; pass it as
 * `observe.sink` with a `level` to record. Writes are single-row inserts off the critical
 * path of the state machine (a failed event write must never fail a run — callers wrap as needed).
 */
export const createPgEventSink = (sql: Sql, schema = "workflow"): EventSink => {
  const t = tables(schema);
  return {
    async record(e: FlowEvent) {
      await sql.query(
        `INSERT INTO ${t.event} (run_id, type, at, data) VALUES ($1, $2, $3, $4::jsonb)`,
        [e.runId, e.type, e.at, j(e.data)],
      );
    },
  };
};

/** Read a run's event timeline, oldest first — the dashboard detail view. */
export const listEvents = async (
  sql: Sql,
  runId: string,
  schema = "workflow",
): Promise<FlowEvent[]> => {
  const t = tables(schema);
  const rows = await sql.query<{ run_id: string; type: EventType; at: Date; data: unknown }>(
    `SELECT run_id, type, at, data FROM ${t.event} WHERE run_id = $1 ORDER BY seq`,
    [runId],
  );
  return rows.map((r) => ({ runId: r.run_id, type: r.type, at: r.at, data: r.data ?? undefined }));
};
