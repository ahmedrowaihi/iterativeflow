/** Normalize an optional injected clock to epoch ms — defaults to now. */
export const ms = (now?: Date): number => (now ?? new Date()).getTime();
