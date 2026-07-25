/**
 * Verify a signed webhook and reduce it to a provider-neutral identity. Uses Web Crypto (no
 * `node:crypto`, no `Buffer`), so it runs unchanged on Node, Workers, and the edge. The raw request
 * BODY must be the exact bytes the provider signed — verify before parsing, since any
 * re-serialization changes the signature.
 */

/** Header lookup that accepts a `Headers` instance or a plain (case-insensitive) record. */
export type HeaderInput = Headers | Record<string, string | string[] | undefined>;

/**
 * A verified webhook, normalized across providers. `id` is unique per delivery (GitHub's
 * `X-GitHub-Delivery`, Svix's `svix-id`) — use it as the idempotency key so a redelivery lands
 * once. `type` is the event name (GitHub's `X-GitHub-Event`).
 */
export interface WebhookEvent<T = unknown> {
  id: string;
  type: string;
  payload: T;
}

export interface VerifyInput {
  /** The raw request body, exactly as received (the bytes the provider signed). */
  body: string;
  headers: HeaderInput;
}

/** Verifies a raw webhook and returns its identity + payload, or throws {@link WebhookError}. A
 *  provider preset (e.g. {@link github}) or {@link hmacVerifier} produces one. */
export type WebhookVerifier = (input: VerifyInput, crypto?: Crypto) => Promise<WebhookEvent>;

export type WebhookErrorCode =
  | "missing_signature"
  | "invalid_signature"
  | "missing_header"
  | "invalid_json";

export class WebhookError extends Error {
  constructor(
    readonly code: WebhookErrorCode,
    message: string,
  ) {
    super(message);
    this.name = "WebhookError";
  }
}

export interface HmacVerifierOptions {
  /** The shared webhook secret configured with the provider. */
  secret: string;
  /** Header carrying the signature, e.g. `x-hub-signature-256`. */
  signatureHeader: string;
  /** Header carrying the unique delivery id (idempotency), e.g. `x-github-delivery`. */
  idHeader: string;
  /** Header carrying the event type, e.g. `x-github-event`. */
  typeHeader: string;
  /** Scheme prefix on the signature value, e.g. `sha256=`. Defaults to none. */
  scheme?: string;
  /** Digest for the HMAC. Defaults to `SHA-256`. */
  hash?: "SHA-256" | "SHA-1";
}

/**
 * A verifier for the common case: an HMAC of the raw body, hex-encoded, in one header. Covers
 * GitHub and any provider with the same shape — point the header/scheme options at theirs.
 *
 * @throws {WebhookError} `missing_signature` / `invalid_signature` when the signature header is
 * absent or doesn't match, `missing_header` when the id/type headers are absent, or `invalid_json`
 * when the body isn't JSON. On any throw the request is untrusted — reject it.
 */
export function hmacVerifier(opts: HmacVerifierOptions): WebhookVerifier {
  const scheme = opts.scheme ?? "";
  const hash = opts.hash ?? "SHA-256";
  return async ({ body, headers }, cryptoImpl) => {
    const signature = headerGet(headers, opts.signatureHeader);
    if (!signature) throw new WebhookError("missing_signature", `missing ${opts.signatureHeader}`);

    const c = cryptoImpl ?? globalThis.crypto;
    const expected = `${scheme}${await hmacHex(c, opts.secret, body, hash)}`;
    if (!timingSafeEqual(expected, signature)) {
      throw new WebhookError("invalid_signature", "webhook signature did not match");
    }

    const id = headerGet(headers, opts.idHeader);
    const type = headerGet(headers, opts.typeHeader);
    if (!id || !type) {
      throw new WebhookError("missing_header", `missing ${opts.idHeader} or ${opts.typeHeader}`);
    }

    let payload: unknown;
    try {
      payload = JSON.parse(body);
    } catch {
      throw new WebhookError("invalid_json", "webhook body was not valid JSON");
    }
    return { id, type, payload };
  };
}

function headerGet(headers: HeaderInput, name: string): string | undefined {
  if (typeof (headers as Headers).get === "function") {
    return (headers as Headers).get(name) ?? undefined;
  }
  const record = headers as Record<string, string | string[] | undefined>;
  const lower = name.toLowerCase();
  for (const key of Object.keys(record)) {
    if (key.toLowerCase() === lower) {
      const value = record[key];
      return Array.isArray(value) ? value[0] : value;
    }
  }
  return undefined;
}

async function hmacHex(
  c: Crypto,
  secret: string,
  body: string,
  hash: "SHA-256" | "SHA-1",
): Promise<string> {
  const enc = new TextEncoder();
  const key = await c.subtle.importKey("raw", enc.encode(secret), { name: "HMAC", hash }, false, [
    "sign",
  ]);
  const mac = await c.subtle.sign("HMAC", key, enc.encode(body));
  const bytes = new Uint8Array(mac);
  let hex = "";
  for (const b of bytes) hex += b.toString(16).padStart(2, "0");
  return hex;
}

function timingSafeEqual(a: string, b: string): boolean {
  if (a.length !== b.length) return false;
  let diff = 0;
  for (let i = 0; i < a.length; i++) diff |= a.charCodeAt(i) ^ b.charCodeAt(i);
  return diff === 0;
}
