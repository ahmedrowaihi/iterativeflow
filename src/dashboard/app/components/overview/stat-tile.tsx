import { StatusDot } from "@/components/ui/status-dot";

export const StatTile = ({
  label,
  value,
  tone,
}: {
  label: string;
  value: number;
  tone?: string;
}) => (
  <div class="uk-card">
    <div class="uk-card-body flex flex-col gap-1">
      <span class="flex items-center gap-1.5 text-xs text-muted-foreground">
        {tone ? <StatusDot tone={`var(--status-${tone})`} /> : null}
        {label}
      </span>
      <span class="text-xl font-semibold">{value}</span>
    </div>
  </div>
);
