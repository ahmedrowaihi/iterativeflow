import { RUN_STATUSES, type RunListItem } from "@/lib/types";
import { StatusDot } from "@/components/ui/status-dot";

export const StatusBar = ({ runs }: { runs: RunListItem[] }) => {
  const total = runs.length || 1;
  const present = RUN_STATUSES.filter((s) => runs.some((r) => r.status === s));
  return (
    <div class="flex flex-col gap-3">
      <div class="flex h-2 overflow-hidden rounded-full bg-muted">
        {present.map((s) => {
          const n = runs.filter((r) => r.status === s).length;
          return (
            <div
              key={s}
              style={`width:${(n / total) * 100}%;background:var(--status-${s})`}
              title={`${s}: ${n}`}
            />
          );
        })}
      </div>
      <div class="flex flex-wrap gap-x-4 gap-y-2">
        {present.map((s) => (
          <span key={s} class="flex items-center gap-1.5 text-xs text-muted-foreground">
            <StatusDot tone={`var(--status-${s})`} />
            {s.replace(/_/g, " ")}{" "}
            <span class="text-foreground">{runs.filter((r) => r.status === s).length}</span>
          </span>
        ))}
      </div>
    </div>
  );
};
