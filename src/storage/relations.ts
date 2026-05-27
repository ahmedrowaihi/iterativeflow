import { relations } from "drizzle-orm";
import { events, hooks, runs, steps, timers } from "./schema";

export const runsRelations = relations(runs, ({ many }) => ({
  steps: many(steps),
  timers: many(timers),
  hooks: many(hooks),
  events: many(events),
}));

export const stepsRelations = relations(steps, ({ one }) => ({
  run: one(runs, { fields: [steps.runId], references: [runs.id] }),
}));

export const timersRelations = relations(timers, ({ one }) => ({
  run: one(runs, { fields: [timers.runId], references: [runs.id] }),
}));

export const hooksRelations = relations(hooks, ({ one }) => ({
  run: one(runs, { fields: [hooks.runId], references: [runs.id] }),
}));

export const eventsRelations = relations(events, ({ one }) => ({
  run: one(runs, { fields: [events.runId], references: [runs.id] }),
}));
