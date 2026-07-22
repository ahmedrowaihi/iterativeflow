// The fixed epoch is load-bearing — every time-based assertion measures against the same instant.
/** @internal */
export const T0 = new Date("2030-01-01T00:00:00Z");

/** @internal */
export const at = (ms: number): Date => new Date(T0.getTime() + ms);
