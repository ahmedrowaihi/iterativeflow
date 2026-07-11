import { useHealth } from "@/lib/queries";
import { StatusDot } from "@/components/ui/status-dot";

const TONE = {
  ok: "var(--status-ok)",
  off: "var(--status-failed)",
  idle: "hsl(var(--muted-foreground))",
} as const;

export const HealthStrip = () => {
  const { data, error } = useHealth();
  const items: [string, keyof typeof TONE][] = error
    ? [["api unreachable", "off"]]
    : data
      ? [
          ["db", data.db ? "ok" : "off"],
          ["worker", data.worker ? "ok" : "idle"],
          ["listen", data.listen ? "ok" : "idle"],
        ]
      : [];
  return (
    <div class="flex items-center gap-3 text-xs text-muted-foreground">
      {items.map(([label, tone]) => (
        <span class="flex items-center gap-1.5">
          <StatusDot tone={TONE[tone]} />
          {label}
        </span>
      ))}
    </div>
  );
};
