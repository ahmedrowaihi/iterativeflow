import { useRun } from "@/lib/queries";
import { closeSheet, sheetRunId } from "@/lib/store";
import { RunActions } from "@/components/common/run-actions";
import { StatusBadge } from "@/components/common/status-badge";
import { RunBody } from "@/components/run/run-body";
import { Button } from "@/components/ui/button";
import { Close } from "@/components/ui/icon";
import { Sheet } from "@/components/ui/sheet";

export const RunSheet = () => {
  const id = sheetRunId.value;
  const { data, error } = useRun(id);
  if (!id) return null;
  return (
    <Sheet onClose={closeSheet}>
      <div class="sticky top-0 z-10 flex items-center gap-2 border-b border-border bg-card px-4 py-2.5">
        {data ? (
          <>
            <h1 class="text-sm font-semibold">
              {data.run.name}{" "}
              <span class="font-normal text-muted-foreground">v{data.run.version}</span>
            </h1>
            <StatusBadge status={data.run.status} />
            <RunActions run={data.run} />
          </>
        ) : null}
        <span class="flex-1" />
        <Button variant="ghost" size="xs" onClick={closeSheet} title="Close">
          <Close />
        </Button>
      </div>
      <RunBody detail={data} error={error} />
    </Sheet>
  );
};
