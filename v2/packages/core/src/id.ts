import { randomUUID } from "node:crypto";

/** Generates a fresh run/child id. Injectable so the runtime can supply ULIDs, KSUIDs, etc. */
export type IdGen = () => string;

/** Default id generator — RFC-4122 v4. Override by passing your own {@link IdGen}. */
export const newId: IdGen = () => randomUUID();
