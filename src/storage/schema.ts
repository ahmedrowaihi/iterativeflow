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

export const RUN_STATUSES = [
  "pending",
  "running",
  "sleeping",
  "waiting",
  "done",
  "failed",
  "canceled",
] as const;
export type RunStatus = (typeof RUN_STATUSES)[number];

export const STEP_STATUSES = ["running", "ok", "failed_retry", "failed_terminal"] as const;
export type StepStatus = (typeof STEP_STATUSES)[number];

export const EVENT_TYPES = [
  "started",
  "step_started",
  "step_ok",
  "step_failed",
  "step_terminal",
  "sleep_scheduled",
  "sleep_fired",
  "hook_armed",
  "hook_resolved",
  "hook_timeout",
  "suspended",
  "resumed",
  "completed",
  "failed",
  "canceled",
] as const;
export type EventType = (typeof EVENT_TYPES)[number];

export const workflowSchema = pgSchema("workflow");

export interface WorkflowError {
  code: string;
  message: string;
  stack?: string;
}

export const runs = workflowSchema.table(
  "runs",
  {
    id: uuid("id").defaultRandom().primaryKey(),
    name: text("name").notNull(),
    version: integer("version").notNull().default(1),
    status: text("status", { enum: RUN_STATUSES }).notNull().default("pending"),
    input: jsonb("input").notNull(),
    output: jsonb("output"),
    error: jsonb("error").$type<WorkflowError>(),
    attempts: integer("attempts").notNull().default(0),
    idempotencyKey: text("idempotency_key"),
    createdAt: timestamp("created_at", { withTimezone: true }).defaultNow().notNull(),
    startedAt: timestamp("started_at", { withTimezone: true }),
    completedAt: timestamp("completed_at", { withTimezone: true }),
    updatedAt: timestamp("updated_at", { withTimezone: true })
      .defaultNow()
      .$onUpdate(() => new Date())
      .notNull(),
  },
  (t) => [
    uniqueIndex("workflow_runs_idempotency_idx")
      .on(t.name, t.idempotencyKey)
      .where(sql`${t.idempotencyKey} IS NOT NULL`),
    index("workflow_runs_status_idx").on(t.status, t.createdAt),
    index("workflow_runs_name_status_idx").on(t.name, t.status),
    index("workflow_runs_reconciler_idx")
      .on(t.updatedAt)
      .where(sql`${t.status} IN ('pending', 'sleeping', 'waiting')`),
  ],
);

export const steps = workflowSchema.table(
  "steps",
  {
    runId: uuid("run_id")
      .notNull()
      .references(() => runs.id, { onDelete: "cascade" }),
    stepKey: text("step_key").notNull(),
    status: text("status", { enum: STEP_STATUSES }).notNull(),
    result: jsonb("result"),
    error: jsonb("error").$type<WorkflowError>(),
    attempts: integer("attempts").notNull().default(0),
    startedAt: timestamp("started_at", { withTimezone: true }).defaultNow().notNull(),
    completedAt: timestamp("completed_at", { withTimezone: true }),
  },
  (t) => [primaryKey({ columns: [t.runId, t.stepKey] })],
);

export const timers = workflowSchema.table(
  "timers",
  {
    runId: uuid("run_id")
      .notNull()
      .references(() => runs.id, { onDelete: "cascade" }),
    stepKey: text("step_key").notNull(),
    fireAt: timestamp("fire_at", { withTimezone: true }).notNull(),
    firedAt: timestamp("fired_at", { withTimezone: true }),
  },
  (t) => [
    primaryKey({ columns: [t.runId, t.stepKey] }),
    index("workflow_timers_fire_at_idx")
      .on(t.fireAt)
      .where(sql`${t.firedAt} IS NULL`),
  ],
);

export const hooks = workflowSchema.table(
  "hooks",
  {
    runId: uuid("run_id")
      .notNull()
      .references(() => runs.id, { onDelete: "cascade" }),
    hookKey: text("hook_key").notNull(),
    delivered: boolean("delivered").notNull().default(false),
    payload: jsonb("payload"),
    expiresAt: timestamp("expires_at", { withTimezone: true }),
    createdAt: timestamp("created_at", { withTimezone: true }).defaultNow().notNull(),
    deliveredAt: timestamp("delivered_at", { withTimezone: true }),
  },
  (t) => [primaryKey({ columns: [t.runId, t.hookKey] })],
);

export const events = workflowSchema.table(
  "events",
  {
    id: bigserial("id", { mode: "number" }).primaryKey(),
    runId: uuid("run_id")
      .notNull()
      .references(() => runs.id, { onDelete: "cascade" }),
    type: text("type", { enum: EVENT_TYPES }).notNull(),
    stepKey: text("step_key"),
    payload: jsonb("payload"),
    at: timestamp("at", { withTimezone: true }).defaultNow().notNull(),
  },
  (t) => [index("workflow_events_run_id_idx").on(t.runId, t.at)],
);

export type RunRow = typeof runs.$inferSelect;
export type NewRunRow = typeof runs.$inferInsert;
export type StepRow = typeof steps.$inferSelect;
export type NewStepRow = typeof steps.$inferInsert;
export type TimerRow = typeof timers.$inferSelect;
export type HookRow = typeof hooks.$inferSelect;
export type EventRow = typeof events.$inferSelect;
