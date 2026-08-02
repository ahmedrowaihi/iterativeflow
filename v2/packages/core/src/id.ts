/** Generates a fresh run/child id. Injectable so the runtime can supply ULIDs, KSUIDs, etc. */
export type IdGen = () => string;

/** Default id generator — RFC-4122 v4 via the Web Crypto global (Node 20+ and browsers, secure
 *  contexts). Override by passing your own {@link IdGen}. */
export const newId: IdGen = () => {
  const webCrypto = globalThis.crypto;
  if (!webCrypto?.randomUUID) {
    throw new Error(
      "iterativeflow: no Web Crypto `crypto.randomUUID` in this runtime — pass a custom `id` (IdGen) to the engine/backend.",
    );
  }
  return webCrypto.randomUUID();
};
