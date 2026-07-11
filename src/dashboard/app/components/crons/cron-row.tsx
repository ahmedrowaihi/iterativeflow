import { openCron, triggerCron } from "@/lib/store";
import type { CronRow as CronData } from "@/lib/types";
import { Button } from "@/components/ui/button";
import { Td, Tr } from "@/components/ui/table";

export const CronRow = ({ cron }: { cron: CronData }) => (
  <Tr>
    <Td class="font-mono">{cron.name}</Td>
    <Td class="font-mono">{cron.schedule}</Td>
    <Td>{cron.timezone}</Td>
    <Td>{cron.overlap}</Td>
    <Td>
      <div class="flex gap-1.5">
        <Button variant="default" size="xs" onClick={() => openCron(cron.name)}>
          View
        </Button>
        <Button size="xs" onClick={() => triggerCron(cron.name)}>
          Run now
        </Button>
      </div>
    </Td>
  </Tr>
);
