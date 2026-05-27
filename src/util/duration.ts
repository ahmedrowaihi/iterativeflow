const UNITS = {
  ms: 1,
  s: 1_000,
  sec: 1_000,
  secs: 1_000,
  second: 1_000,
  seconds: 1_000,
  m: 60_000,
  min: 60_000,
  mins: 60_000,
  minute: 60_000,
  minutes: 60_000,
  h: 3_600_000,
  hr: 3_600_000,
  hrs: 3_600_000,
  hour: 3_600_000,
  hours: 3_600_000,
  d: 86_400_000,
  day: 86_400_000,
  days: 86_400_000,
  w: 604_800_000,
  wk: 604_800_000,
  wks: 604_800_000,
  week: 604_800_000,
  weeks: 604_800_000,
  y: 31_557_600_000,
  yr: 31_557_600_000,
  yrs: 31_557_600_000,
  year: 31_557_600_000,
  years: 31_557_600_000,
} as const satisfies Record<string, number>;

type Unit = keyof typeof UNITS;

export type DurationString = `${number}${Unit}` | `${number} ${Unit}`;
export type Duration = number | DurationString | Date;

const DURATION_RE = new RegExp(
  `^(-?\\d+(?:\\.\\d+)?)\\s*(${Object.keys(UNITS)
    .sort((a, b) => b.length - a.length)
    .join("|")})$`,
  "i",
);

const parseDurationString = (s: string): number => {
  const m = DURATION_RE.exec(s.trim());
  if (!m) throw new Error(`Invalid duration: ${s}`);
  return Math.round(Number(m[1]) * UNITS[m[2].toLowerCase() as Unit]);
};

export const toMs = (duration: Duration): number => {
  if (duration instanceof Date) return Math.max(0, duration.getTime() - Date.now());
  if (typeof duration === "number") return duration;
  return parseDurationString(duration);
};

export const toFireAt = (duration: Duration): Date => {
  if (duration instanceof Date) return duration;
  return new Date(Date.now() + toMs(duration));
};
