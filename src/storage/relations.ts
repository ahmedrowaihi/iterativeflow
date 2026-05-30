import { relations } from "drizzle-orm";
import { events, runs, signals, steps, timers } from "./schema";

export const runsRelations = relations(runs, ({ many, one }) => ({
  steps: many(steps),
  timers: many(timers),
  signals: many(signals),
  events: many(events),
  parent: one(runs, {
    fields: [runs.parentRunId],
    references: [runs.id],
    relationName: "parent",
  }),
  children: many(runs, { relationName: "parent" }),
}));

export const stepsRelations = relations(steps, ({ one }) => ({
  run: one(runs, { fields: [steps.runId], references: [runs.id] }),
}));

export const timersRelations = relations(timers, ({ one }) => ({
  run: one(runs, { fields: [timers.runId], references: [runs.id] }),
}));

export const signalsRelations = relations(signals, ({ one }) => ({
  run: one(runs, { fields: [signals.runId], references: [runs.id] }),
}));

export const eventsRelations = relations(events, ({ one }) => ({
  run: one(runs, { fields: [events.runId], references: [runs.id] }),
}));
