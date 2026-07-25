import { describe, expect, it } from "vitest";
import { github } from "#github";
import { WebhookError, hmacVerifier } from "#verify";

const SECRET = "s3cr3t";

async function ghSign(body: string, secret = SECRET): Promise<string> {
  const enc = new TextEncoder();
  const key = await crypto.subtle.importKey(
    "raw",
    enc.encode(secret),
    { name: "HMAC", hash: "SHA-256" },
    false,
    ["sign"],
  );
  const mac = await crypto.subtle.sign("HMAC", key, enc.encode(body));
  const hex = [...new Uint8Array(mac)].map((b) => b.toString(16).padStart(2, "0")).join("");
  return `sha256=${hex}`;
}

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
    const event = await verify({ body, headers: ghHeaders(await ghSign(body)) });
    expect(event).toEqual({ id: "guid-1", type: "issue_comment", payload: expect.anything() });
    expect((event.payload as { number: number }).number).toBe(7);
  });

  it("reads headers case-insensitively and from a Headers instance", async () => {
    const body = JSON.stringify({ ok: true });
    const h = new Headers({
      "x-hub-signature-256": await ghSign(body),
      "x-github-event": "push",
      "x-github-delivery": "guid-2",
    });
    const event = await verify({ body, headers: h });
    expect(event.type).toBe("push");
    expect(event.id).toBe("guid-2");
  });

  it("rejects a tampered body", async () => {
    const sig = await ghSign(JSON.stringify({ n: 1 }));
    await expect(
      verify({ body: JSON.stringify({ n: 2 }), headers: ghHeaders(sig) }),
    ).rejects.toMatchObject({ code: "invalid_signature" });
  });

  it("rejects a signature from the wrong secret", async () => {
    const body = JSON.stringify({ n: 1 });
    await expect(
      verify({ body, headers: ghHeaders(await ghSign(body, "wrong")) }),
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
      verify({ body, headers: { "X-Hub-Signature-256": await ghSign(body) } }),
    ).rejects.toMatchObject({ code: "missing_header" });
  });

  it("rejects a non-JSON body (even when correctly signed)", async () => {
    const body = "not json";
    await expect(verify({ body, headers: ghHeaders(await ghSign(body)) })).rejects.toMatchObject({
      code: "invalid_json",
    });
  });
});

describe("hmacVerifier (generic building block)", () => {
  it("supports a no-scheme, custom-header provider", async () => {
    const secret = "abc";
    const body = JSON.stringify({ hello: "world" });
    const enc = new TextEncoder();
    const key = await crypto.subtle.importKey(
      "raw",
      enc.encode(secret),
      { name: "HMAC", hash: "SHA-256" },
      false,
      ["sign"],
    );
    const mac = await crypto.subtle.sign("HMAC", key, enc.encode(body));
    const hex = [...new Uint8Array(mac)].map((b) => b.toString(16).padStart(2, "0")).join("");

    const verify = hmacVerifier({
      secret,
      signatureHeader: "x-signature",
      idHeader: "x-event-id",
      typeHeader: "x-event-type",
    });
    const event = await verify({
      body,
      headers: { "x-signature": hex, "x-event-id": "e1", "x-event-type": "thing.happened" },
    });
    expect(event).toMatchObject({ id: "e1", type: "thing.happened" });
  });
});
