import { type WebhookVerifier, hmacVerifier } from "#verify";

/** GitHub App / repo webhook verifier — HMAC-SHA256 with the `sha256=` scheme over the raw body. */
export const github = (secret: string): WebhookVerifier =>
  hmacVerifier({
    secret,
    signatureHeader: "x-hub-signature-256",
    idHeader: "x-github-delivery",
    typeHeader: "x-github-event",
    scheme: "sha256=",
  });
