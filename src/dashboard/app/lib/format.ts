export const shortId = (id: string): string => String(id).slice(0, 8);

export const fmtAbs = (iso?: string | null): string => (iso ? new Date(iso).toLocaleString() : "—");

const rtf = new Intl.RelativeTimeFormat(undefined, { numeric: "auto" });
const REL_UNITS: [Intl.RelativeTimeFormatUnit, number][] = [
  ["year", 31_536_000],
  ["day", 86_400],
  ["hour", 3600],
  ["minute", 60],
  ["second", 1],
];

export const rel = (iso?: string | null): string => {
  if (!iso) return "—";
  const deltaSec = (new Date(iso).getTime() - Date.now()) / 1000;
  const abs = Math.abs(deltaSec);
  for (const [unit, secs] of REL_UNITS) {
    if (abs >= secs || unit === "second") return rtf.format(Math.round(deltaSec / secs), unit);
  }
  return "—";
};

export const dur = (fromIso?: string | null, toIso?: string | null): string => {
  if (!fromIso) return "—";
  const ms = (toIso ? new Date(toIso) : new Date()).getTime() - new Date(fromIso).getTime();
  if (ms < 0) return "—";
  const s = Math.round(ms / 1000);
  if (s < 60) return `${s}s`;
  if (s < 3600) return `${Math.floor(s / 60)}m ${s % 60}s`;
  return `${Math.floor(s / 3600)}h ${Math.floor((s % 3600) / 60)}m`;
};
