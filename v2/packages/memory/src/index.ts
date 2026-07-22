import { createMemoryBackend } from "#backend";

export { createMemoryBackend };

/** Standalone in-memory {@link Store} (its own backend). For isolated store conformance. */
export const createMemoryStore = () => createMemoryBackend().store;
/** Standalone in-memory {@link Queue} (its own backend). For isolated queue conformance. */
export const createMemoryQueue = () => createMemoryBackend().queue;
/** Standalone in-memory {@link Timer} (its own backend). For isolated timer conformance. */
export const createMemoryTimer = () => createMemoryBackend().timer;
/** Standalone in-memory {@link Wakeup} (its own backend). For isolated wakeup conformance. */
export const createMemoryWakeup = () => createMemoryBackend().wakeup;
