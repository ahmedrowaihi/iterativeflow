import type { FlowError } from "#types";

/** A JSON value: what every backend stores a run's input, step results and signal payloads as. */
export type Json = string | number | boolean | null | Json[] | { [key: string]: Json };

/**
 * Round-trip a value through JSON. A backend that keeps rich types (memory, BSON) uses this so a
 * `Date`, `Map` or `Set` comes back exactly as it would from a JSON column on every other backend.
 */
export const durable = <T>(value: T): Json | undefined =>
  value === undefined ? undefined : JSON.parse(JSON.stringify(value));

type JsonObject = { [key: string]: Json };

/** Whether a stored JSON value is a string. */
export const isJsonString = (value: Json | undefined): value is string => value === String(value);

const isJsonObject = (value: Json): value is JsonObject =>
  value !== null && !Array.isArray(value) && Object(value) === value;

/**
 * Decode a stored {@link FlowError}. `where` names the column or field for the error message.
 * @throws when the value is present but isn't a flow error.
 */
export const decodeFlowError = (value: Json | undefined, where: string): FlowError | undefined => {
  if (value === undefined || value === null) return undefined;
  if (!isJsonObject(value) || !isJsonString(value.code) || !isJsonString(value.message)) {
    throw new Error(`${where} is not a flow error`);
  }
  const error: FlowError = { code: value.code, message: value.message };
  if (isJsonString(value.stack)) error.stack = value.stack;
  if (isJsonString(value.cause)) error.cause = value.cause;
  return error;
};

/**
 * Decode a stored tag list.
 * @throws when the value is present but isn't a list of strings.
 */
export const decodeTags = (value: Json | undefined, where: string): string[] | undefined => {
  if (value === undefined || value === null) return undefined;
  if (!Array.isArray(value) || !value.every(isJsonString)) {
    throw new Error(`${where} is not a string list`);
  }
  return value;
};

/**
 * Narrow a stored string to one of `allowed` (a status list such as `RUN_STATUSES`).
 * @throws when the value is not in the list.
 */
export const decodeOneOf = <T extends string>(
  allowed: readonly T[],
  value: string,
  where: string,
): T => {
  const hit = allowed.find((a) => a === value);
  if (hit === undefined) throw new Error(`${where} holds unknown value "${value}"`);
  return hit;
};
