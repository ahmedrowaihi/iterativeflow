import { Badge } from "@/components/ui/badge";

export const StatusBadge = ({ status, label }: { status: string; label?: string }) => (
  <Badge variant="status" data-status={status}>
    <span class="status-dot" aria-hidden="true" />
    {label ?? status.replace(/_/g, " ")}
  </Badge>
);
