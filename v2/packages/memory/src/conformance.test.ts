import {
  cronConformance,
  engineConformance,
  outboxConformance,
  queueConformance,
  reconcileConformance,
  signalConformance,
  storeConformance,
  timerConformance,
  wakeupConformance,
} from "@iterativeflow/conformance";
import {
  createMemoryBackend,
  createMemoryQueue,
  createMemoryStore,
  createMemoryTimer,
  createMemoryWakeup,
} from "#index";

// The in-memory backend must satisfy the full port contracts. Postgres and DynamoDB call
// these same suites — one spec, every implementation.
storeConformance("memory", () => createMemoryStore());
queueConformance("memory", () => createMemoryQueue());
timerConformance("memory", () => createMemoryTimer());
wakeupConformance("memory", () => createMemoryWakeup());
outboxConformance("memory", () => createMemoryBackend());
signalConformance("memory", () => createMemoryBackend());
reconcileConformance("memory", () => createMemoryBackend());
cronConformance("memory", () => createMemoryStore());
engineConformance("memory", () => createMemoryBackend());
