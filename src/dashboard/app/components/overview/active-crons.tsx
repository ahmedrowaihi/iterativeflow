import type { CronRow } from "@/lib/types";
import { Section } from "@/components/ui/card";

export const ActiveCrons = ({ crons }: { crons: CronRow[] }) => (
  <Section title="Active crons">
    {crons.length ? (
      <ul class="flex flex-col gap-2">
        {crons.map((c) => (
          <li key={c.name} class="flex items-center gap-3 text-sm">
            <span class="font-mono">{c.name}</span>
            <span class="font-mono text-muted-foreground">{c.schedule}</span>
            <span class="text-xs text-muted-foreground">{c.timezone}</span>
          </li>
        ))}
      </ul>
    ) : (
      <p class="text-xs text-muted-foreground">No crons registered.</p>
    )}
  </Section>
);
