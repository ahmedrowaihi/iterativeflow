import { cronTag } from "@/lib/contract";
import { useCrons } from "@/lib/queries";
import { closeCronSheet, sheetCronName, triggerCron } from "@/lib/store";
import type { Filters } from "@/lib/types";
import { CronTriggerResult } from "@/components/crons/cron-trigger-result";
import { RunsList } from "@/components/runs/runs-list";
import { Button } from "@/components/ui/button";
import { Close } from "@/components/ui/icon";
import { Sheet } from "@/components/ui/sheet";

export const CronSheet = () => {
  const name = sheetCronName.value;
  const { data } = useCrons();
  if (!name) return null;
  const cron = data?.crons.find((c) => c.name === name);
  const runFilters: Filters = { name: "", status: "", tag: cronTag(name), since: "", until: "" };
  return (
    <Sheet onClose={closeCronSheet}>
      <div class="sticky top-0 z-10 flex items-center gap-2 border-b border-border bg-card px-4 py-2.5">
        <h1 class="font-mono text-sm font-semibold">{name}</h1>
        {cron ? (
          <span class="text-xs text-muted-foreground">
            {cron.schedule} · {cron.timezone} · overlap {cron.overlap}
          </span>
        ) : null}
        <Button size="xs" onClick={() => triggerCron(name)}>
          Run now
        </Button>
        <span class="flex-1" />
        <Button variant="ghost" size="xs" onClick={closeCronSheet} title="Close">
          <Close />
        </Button>
      </div>
      <div class="flex flex-col gap-3 p-4">
        <CronTriggerResult name={name} />
        <RunsList
          filters={runFilters}
          emptyLabel={`No runs tagged ${cronTag(name)}. Tag runs this cron starts to see them here.`}
        />
      </div>
    </Sheet>
  );
};
