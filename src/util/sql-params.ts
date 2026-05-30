import { sql, type SQL } from "drizzle-orm";

/**
 * Bind a JS `Date` to a raw `sql\`\`` fragment as a `timestamptz`.
 *
 * Raw `sql\`\${col} < \${date}\`` doesn't propagate column type info, so
 * postgres-js / neon-serverless can't encode `Date` as a positional
 * parameter (only `node-postgres` does that). Wrapping with `ts(date)`
 * emits `'2025-01-15T12:34:56.789Z'::timestamptz`, which every driver
 * accepts and Postgres parses to the same absolute UTC instant.
 *
 * Always use `ts()` (or drizzle's typed helpers like `lt(col, date)` when
 * a column ref is on the other side) whenever a JS `Date` lands in a raw
 * sql fragment.
 *
 * @internal
 */
export const ts = (value: Date): SQL => sql`${value.toISOString()}::timestamptz`;
