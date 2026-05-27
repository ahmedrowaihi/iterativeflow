/* eslint-disable @typescript-eslint/no-explicit-any */
import type { PgDatabase, PgQueryResultHKT } from "drizzle-orm/pg-core";

/**
 * Drizzle Postgres database accepted by the workflow engine. Loose generics
 * keep this compatible with both drizzle 0.x and 1.x — the engine only
 * touches its own `workflow.*` tables, so the consumer's schema/relations
 * shape doesn't matter to it.
 */
export type WorkflowDb = PgDatabase<PgQueryResultHKT, any, any>;
