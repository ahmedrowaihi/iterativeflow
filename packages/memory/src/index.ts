import { createMemoryBackend } from "#backend";

export { createMemoryBackend };

/**
 * An isolated in-memory {@link Store} on its own single-port backend: it shares no state with any
 * other port, so it is only for testing the store alone.
 */
export const createMemoryStore = () => createMemoryBackend().store;
/**
 * An isolated in-memory {@link Queue} on its own single-port backend: it shares no state with any
 * other port, so it is only for testing the queue alone.
 */
export const createMemoryQueue = () => createMemoryBackend().queue;
/**
 * An isolated in-memory {@link Timer} on its own single-port backend: it shares no state with any
 * other port, so it is only for testing the timer alone.
 */
export const createMemoryTimer = () => createMemoryBackend().timer;
/**
 * An isolated in-memory {@link Wakeup} on its own single-port backend: it shares no state with any
 * other port, so it is only for testing the wakeup alone.
 */
export const createMemoryWakeup = () => createMemoryBackend().wakeup;
