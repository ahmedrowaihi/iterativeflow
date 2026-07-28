import { describe, expect, it } from "vitest";
import { pgClassify } from "#classify";

const withCode = (code: string): Error => Object.assign(new Error("boom"), { code });

describe("pgClassify", () => {
  it("fails fast on deterministic errors", () => {
    expect(pgClassify(withCode("23502"))).toBe("permanent"); // not-null violation
    expect(pgClassify(withCode("23514"))).toBe("permanent"); // check violation
    expect(pgClassify(withCode("22P02"))).toBe("permanent"); // invalid text representation
    expect(pgClassify(withCode("42703"))).toBe("permanent"); // undefined column
  });

  it("retries connection / timeout / deadlock / serialization", () => {
    expect(pgClassify(withCode("08006"))).toBe("transient"); // connection failure
    expect(pgClassify(withCode("57014"))).toBe("transient"); // canceling statement due to timeout
    expect(pgClassify(withCode("40P01"))).toBe("transient"); // deadlock detected
    expect(pgClassify(withCode("40001"))).toBe("transient"); // could not serialize
  });

  it("keeps foreign-key / unique violations transient (they can be a concurrency race)", () => {
    expect(pgClassify(withCode("23503"))).toBe("transient"); // foreign_key_violation
    expect(pgClassify(withCode("23505"))).toBe("transient"); // unique_violation
  });

  it("walks the .cause chain a driver wraps", () => {
    const wrapped = new Error("Failed query: rollback", { cause: withCode("23502") });
    expect(pgClassify(wrapped)).toBe("permanent");
  });

  it("defaults an unrecognized error to transient", () => {
    expect(pgClassify(new Error("connection terminated unexpectedly"))).toBe("transient");
    expect(pgClassify(withCode("XX000"))).toBe("transient"); // internal_error — retry
  });
});
