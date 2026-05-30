/** @internal */
export const enforcePayloadCap = (label: string, value: unknown, cap: number | undefined): void => {
  if (cap === undefined || value === undefined || value === null) return;
  const bytes = Buffer.byteLength(JSON.stringify(value), "utf8");
  if (bytes > cap) {
    throw new Error(`${label} exceeds limit: ${bytes} > ${cap} bytes`);
  }
};
