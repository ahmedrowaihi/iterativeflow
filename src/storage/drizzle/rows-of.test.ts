import { describe, expect, it } from "vitest";
import { rowsOf } from "./types";

describe("rowsOf", () => {
  it("returns `rows` when given a node-postgres-style QueryResult", () => {
    expect(rowsOf({ rows: [{ a: 1 }, { a: 2 }] })).toEqual([{ a: 1 }, { a: 2 }]);
  });

  it("returns the array directly when given a postgres-js / drizzle-1.x shape", () => {
    expect(rowsOf([{ a: 1 }, { a: 2 }])).toEqual([{ a: 1 }, { a: 2 }]);
  });

  it("returns [] when `rows` is missing", () => {
    expect(rowsOf({})).toEqual([]);
  });
});
