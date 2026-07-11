import { cancelRun, retryRun } from "@/lib/store";
import { ACTIVE_STATUSES, type RunListItem } from "@/lib/types";
import { Button } from "@/components/ui/button";

export const RunActions = ({ run, placeholder }: { run: RunListItem; placeholder?: boolean }) => {
  const canCancel = ACTIVE_STATUSES.has(run.status);
  const canRetry = run.status === "failed";
  if (!canCancel && !canRetry)
    return placeholder ? <span class="text-muted-foreground">—</span> : null;
  return (
    <div class="flex gap-2">
      {canCancel ? (
        <Button
          variant="destructive"
          onClick={(e) => {
            e.stopPropagation();
            cancelRun(run);
          }}
        >
          Cancel
        </Button>
      ) : null}
      {canRetry ? (
        <Button
          onClick={(e) => {
            e.stopPropagation();
            retryRun(run);
          }}
        >
          Retry
        </Button>
      ) : null}
    </div>
  );
};
