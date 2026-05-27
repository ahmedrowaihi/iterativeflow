import type { StandardSchemaV1 } from "@standard-schema/spec";

export type { StandardSchemaV1 };

export const validate = async <T>(
  schema: StandardSchemaV1<unknown, T>,
  value: unknown,
): Promise<StandardSchemaV1.Result<T>> => {
  const r = schema["~standard"].validate(value);
  return r instanceof Promise ? await r : r;
};

export const formatIssues = (issues: ReadonlyArray<StandardSchemaV1.Issue>): string =>
  issues
    .map((i) => {
      const path = i.path
        ?.map((p) => (typeof p === "object" ? String(p.key) : String(p)))
        .join(".");
      return path ? `${path}: ${i.message}` : i.message;
    })
    .join("; ");
