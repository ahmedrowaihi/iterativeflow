/** The clock the executor threads in — injectable for deterministic tests. */
export type Clock = () => Date;

/** Wall-clock default. Passed where a deployment doesn't inject its own {@link Clock}. */
export const systemClock: Clock = () => new Date();
