import { createHash } from "node:crypto";
import { describe, expect, it } from "vitest";
import { sha256hex } from "#engine/sha256";

describe("sha256hex", () => {
  it("matches the SHA-256 known-answer vectors", () => {
    expect(sha256hex("")).toBe("e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855");
    expect(sha256hex("abc")).toBe(
      "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad",
    );
  });

  it("equals node:crypto sha256 across payloads that cross the 64-byte block boundary", () => {
    for (const seed of [
      "a",
      "runId:cursor.step[3]",
      "x".repeat(55),
      "x".repeat(64),
      "y".repeat(120),
    ]) {
      const expected = createHash("sha256").update(seed).digest("hex");
      expect(sha256hex(seed)).toBe(expected);
    }
  });
});
