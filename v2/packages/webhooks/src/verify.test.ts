import { createHmac } from "node:crypto";
import { describe, expect, it } from "vitest";
import { github } from "#github";
import { WebhookError, hmacVerifier } from "#verify";

const SECRET = "s3cr3t";

// An INDEPENDENT signer (node:crypto, not the verifier's own Web Crypto path), so a bug in the
// implementation's hashing can't mask itself by signing and verifying with the same code.
const hmac = (body: string, secret: string, algo = "sha256"): string =>
  createHmac(algo, secret).update(body).digest("hex");
const ghSign = (body: string, secret = SECRET): string => `sha256=${hmac(body, secret)}`;

const ghHeaders = (sig: string, extra: Record<string, string> = {}): Record<string, string> => ({
  "X-Hub-Signature-256": sig,
  "X-GitHub-Event": "issue_comment",
  "X-GitHub-Delivery": "guid-1",
  ...extra,
});

describe("github preset verifier", () => {
  const verify = github(SECRET);

  it("accepts a correctly signed body, normalizing to id/type/payload", async () => {
    const body = JSON.stringify({ action: "created", number: 7 });
    const event = await verify({ body, headers: ghHeaders(ghSign(body)) });
    expect(event).toEqual({ id: "guid-1", type: "issue_comment", payload: expect.anything() });
    expect((event.payload as { number: number }).number).toBe(7);
  });

  it("reads headers case-insensitively and from a Headers instance", async () => {
    const body = JSON.stringify({ ok: true });
    const h = new Headers({
      "x-hub-signature-256": ghSign(body),
      "x-github-event": "push",
      "x-github-delivery": "guid-2",
    });
    const event = await verify({ body, headers: h });
    expect(event.type).toBe("push");
    expect(event.id).toBe("guid-2");
  });

  it("rejects a tampered body", async () => {
    const sig = ghSign(JSON.stringify({ n: 1 }));
    await expect(
      verify({ body: JSON.stringify({ n: 2 }), headers: ghHeaders(sig) }),
    ).rejects.toMatchObject({ code: "invalid_signature" });
  });

  it("rejects a signature from the wrong secret", async () => {
    const body = JSON.stringify({ n: 1 });
    await expect(
      verify({ body, headers: ghHeaders(ghSign(body, "wrong")) }),
    ).rejects.toBeInstanceOf(WebhookError);
  });

  it("rejects a missing signature header", async () => {
    await expect(
      verify({
        body: JSON.stringify({ n: 1 }),
        headers: { "X-GitHub-Event": "push", "X-GitHub-Delivery": "g" },
      }),
    ).rejects.toMatchObject({ code: "missing_signature" });
  });

  it("rejects when id/type headers are absent", async () => {
    const body = JSON.stringify({ n: 1 });
    await expect(
      verify({ body, headers: { "X-Hub-Signature-256": ghSign(body) } }),
    ).rejects.toMatchObject({ code: "missing_header" });
  });

  it("rejects a non-JSON body (even when correctly signed)", async () => {
    const body = "not json";
    await expect(verify({ body, headers: ghHeaders(ghSign(body)) })).rejects.toMatchObject({
      code: "invalid_json",
    });
  });
});

describe("hmacVerifier (generic building block)", () => {
  it("refuses to build with an empty secret (fails closed, not open)", () => {
    expect(() =>
      hmacVerifier({ secret: "", signatureHeader: "x", idHeader: "y", typeHeader: "z" }),
    ).toThrow(/non-empty/);
    expect(() => github("")).toThrow(/non-empty/);
  });

  it("verifies a SHA-1 provider when configured", async () => {
    const secret = "sha1-secret";
    const body = JSON.stringify({ a: 1 });
    const hex = hmac(body, secret, "sha1");
    const verify = hmacVerifier({
      secret,
      signatureHeader: "x-sig",
      idHeader: "x-id",
      typeHeader: "x-type",
      hash: "SHA-1",
    });
    const event = await verify({ body, headers: { "x-sig": hex, "x-id": "s1", "x-type": "t" } });
    expect(event).toMatchObject({ id: "s1", type: "t" });
    await expect(
      verify({
        body: JSON.stringify({ a: 2 }),
        headers: { "x-sig": hex, "x-id": "s1", "x-type": "t" },
      }),
    ).rejects.toMatchObject({ code: "invalid_signature" });
  });

  it("reads the first value of an array-valued signature header", async () => {
    const secret = "arr";
    const body = JSON.stringify({ a: 1 });
    const verify = hmacVerifier({
      secret,
      signatureHeader: "x-sig",
      idHeader: "x-id",
      typeHeader: "x-type",
    });
    const event = await verify({
      body,
      headers: { "x-sig": [hmac(body, secret), "other"], "x-id": ["a1"], "x-type": ["t"] },
    });
    expect(event).toMatchObject({ id: "a1", type: "t" });
  });

  it("supports a no-scheme, custom-header provider", async () => {
    const secret = "abc";
    const body = JSON.stringify({ hello: "world" });
    const verify = hmacVerifier({
      secret,
      signatureHeader: "x-signature",
      idHeader: "x-event-id",
      typeHeader: "x-event-type",
    });
    const event = await verify({
      body,
      headers: {
        "x-signature": hmac(body, secret),
        "x-event-id": "e1",
        "x-event-type": "thing.happened",
      },
    });
    expect(event).toMatchObject({ id: "e1", type: "thing.happened" });
  });
});
