import { getTableConfig } from "drizzle-orm/pg-core";
import { sql } from "drizzle-orm";
import { rowsOf, type StorageSliceDeps } from "./types";

/**
 * Probe `information_schema.tables` for every consumer-supplied table at its
 * configured `(schema, name)`. Returns `2` when all are present, `0` if any
 * are missing.
 *
 * @internal
 */
export const getSchemaVersion =
  ({ db, tables }: StorageSliceDeps) =>
  async (): Promise<number> => {
    const probes = [tables.runs, tables.steps, tables.signals, tables.timers, tables.events];
    for (const tbl of probes) {
      const cfg = getTableConfig(tbl);
      const rows = rowsOf<{ present: boolean }>(
        await db.execute(sql`
          SELECT EXISTS(
            SELECT 1 FROM information_schema.tables
            WHERE table_schema = ${cfg.schema ?? "public"}
              AND table_name = ${cfg.name}
          ) AS present
        `),
      );
      if (!rows[0]?.present) return 0;
    }
    return 2;
  };
