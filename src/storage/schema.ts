import { sql } from "drizzle-orm";
import {
  bigserial,
  boolean,
  index,
  integer,
  jsonb,
  pgSchema,
  primaryKey,
  text,
  timestamp,
  uniqueIndex,
  uuid,
} from "drizzle-orm/pg-core";

/** Run lifecycle states. Terminal: `done`, `failed`, `canceled`. */
export const RUN_STATUSES = [
  "pending",
  "running",
  "sleeping",
  "awaiting_signal",
  "retrying",
  "done",
  "failed",
  "canceled",
] as const;
/** Run lifecycle states. Terminal: `done`, `failed`, `canceled`. */
export type RunStatus = (typeof RUN_STATUSES)[number];

/** Step lifecycle states. Terminal: `ok`, `failed_terminal`. */
export const STEP_STATUSES = ["running", "ok", "failed_retry", "failed_terminal"] as const;
/** Step lifecycle states. Terminal: `ok`, `failed_terminal`. */
export type StepStatus = (typeof STEP_STATUSES)[number];

/** Event types recorded in `workflow.events` (audit trail). */
export const EVENT_TYPES = [
  "started",
  "step_started",
  "step_ok",
  "step_failed",
  "step_terminal",
  "sleep_scheduled",
  "sleep_fired",
  "signal_armed",
  "signal_delivered",
  "signal_timeout",
  "suspended",
  "resumed",
  "completed",
  "failed",
  "canceled",
] as const;
/** Event types recorded in `workflow.events` (audit trail). */
export type EventType = (typeof EVENT_TYPES)[number];

/** Drizzle handle for the `workflow` Postgres schema (tables, columns, indexes). */
export const flowSchema = pgSchema("workflow");

/** Engine-known error codes. User-defined codes are also valid via the open string union. */
export const FLOW_ERROR_CODES = [
  "STEP_FAILED",
  "STEP_RESULT_TOO_LARGE",
  "STEP_INVALID_AWAIT",
  "SIGNAL_PAYLOAD_INVALID",
  "SIGNAL_TIMEOUT",
  "FLOW_UNKNOWN",
  "INPUT_INVALID",
  "RUN_CANCELED",
  "RUN_ATTEMPTS_EXHAUSTED",
  "INVOKE_DEPTH_EXCEEDED",
  "INVOKE_FANOUT_EXCEEDED",
  "REPLAY_NON_DETERMINISTIC",
  "REPLAY_INCOMPATIBLE_VERSION",
  "SCHEMA_MISMATCH",
] as const;
/** Engine-known error codes; open string union allows user-defined codes. */
export type FlowErrorCode = (typeof FLOW_ERROR_CODES)[number] | (string & {});
/** Structured error payload persisted on failed runs and emitted in failure events. */
export interface FlowError {
  /** Stable error code (engine-known or user-defined). */
  code: FlowErrorCode;
  /** Human-readable description. */
  message: string;
  /** Optional captured stack trace. */
  stack?: string;
}

export const runs = flowSchema.table(
  "runs",
  {
    id: uuid("id").defaultRandom().primaryKey(),
    name: text("name").notNull(),
    version: integer("version").notNull().default(1),
    status: text("status", { enum: RUN_STATUSES }).notNull().default("pending"),
    parentRunId: uuid("parent_run_id"),
    parentCursorKey: text("parent_cursor_key"),
    input: jsonb("input").notNull(),
    output: jsonb("output"),
    error: jsonb("error").$type<FlowError>(),
    attempts: integer("attempts").notNull().default(0),
    idempotencyKey: text("idempotency_key"),
    tags: text("tags").array(),
    createdAt: timestamp("created_at", { withTimezone: true }).defaultNow().notNull(),
    startedAt: timestamp("started_at", { withTimezone: true }),
    completedAt: timestamp("completed_at", { withTimezone: true }),
    updatedAt: timestamp("updated_at", { withTimezone: true })
      .defaultNow()
      .$onUpdate(() => new Date())
      .notNull(),
  },
  (t) => [
    uniqueIndex("flow_runs_idempotency_idx")
      .on(t.name, t.version, t.idempotencyKey)
      .where(sql`${t.idempotencyKey} IS NOT NULL`),
    index("flow_runs_status_idx").on(t.status, t.createdAt),
    index("flow_runs_name_status_idx").on(t.name, t.status),
    index("flow_runs_reconciler_idx")
      .on(t.updatedAt)
      .where(sql`${t.status} IN ('pending', 'sleeping', 'awaiting_signal', 'retrying')`),
    index("flow_runs_parent_idx")
      .on(t.parentRunId)
      .where(sql`${t.parentRunId} IS NOT NULL`),
    index("flow_runs_tags_gin_idx").using("gin", t.tags),
  ],
);

export const steps = flowSchema.table(
  "steps",
  {
    runId: uuid("run_id")
      .notNull()
      .references(() => runs.id, { onDelete: "cascade" }),
    cursorKey: text("cursor_key").notNull(),
    status: text("status", { enum: STEP_STATUSES }).notNull(),
    result: jsonb("result"),
    error: jsonb("error").$type<FlowError>(),
    attempts: integer("attempts").notNull().default(0),
    startedAt: timestamp("started_at", { withTimezone: true }).defaultNow().notNull(),
    completedAt: timestamp("completed_at", { withTimezone: true }),
  },
  (t) => [primaryKey({ columns: [t.runId, t.cursorKey] })],
);

export const timers = flowSchema.table(
  "timers",
  {
    runId: uuid("run_id")
      .notNull()
      .references(() => runs.id, { onDelete: "cascade" }),
    cursorKey: text("cursor_key").notNull(),
    fireAt: timestamp("fire_at", { withTimezone: true }).notNull(),
    firedAt: timestamp("fired_at", { withTimezone: true }),
  },
  (t) => [
    primaryKey({ columns: [t.runId, t.cursorKey] }),
    index("flow_timers_fire_at_idx")
      .on(t.fireAt)
      .where(sql`${t.firedAt} IS NULL`),
  ],
);

export const signals = flowSchema.table(
  "signals",
  {
    runId: uuid("run_id")
      .notNull()
      .references(() => runs.id, { onDelete: "cascade" }),
    cursorKey: text("cursor_key").notNull(),
    delivered: boolean("delivered").notNull().default(false),
    payload: jsonb("payload"),
    expiresAt: timestamp("expires_at", { withTimezone: true }),
    createdAt: timestamp("created_at", { withTimezone: true }).defaultNow().notNull(),
    deliveredAt: timestamp("delivered_at", { withTimezone: true }),
  },
  (t) => [primaryKey({ columns: [t.runId, t.cursorKey] })],
);

export const events = flowSchema.table(
  "events",
  {
    id: bigserial("id", { mode: "number" }).primaryKey(),
    runId: uuid("run_id")
      .notNull()
      .references(() => runs.id, { onDelete: "cascade" }),
    type: text("type", { enum: EVENT_TYPES }).notNull(),
    cursorKey: text("cursor_key"),
    payload: jsonb("payload"),
    at: timestamp("at", { withTimezone: true }).defaultNow().notNull(),
  },
  (t) => [index("flow_events_run_id_idx").on(t.runId, t.at)],
);

export const flowTables = { runs, steps, signals, timers, events } as const;

export type RunRow = typeof runs.$inferSelect;
export type NewRunRow = typeof runs.$inferInsert;
export type StepRow = typeof steps.$inferSelect;
export type NewStepRow = typeof steps.$inferInsert;
export type TimerRow = typeof timers.$inferSelect;
export type SignalRow = typeof signals.$inferSelect;
export type EventRow = typeof events.$inferSelect;
