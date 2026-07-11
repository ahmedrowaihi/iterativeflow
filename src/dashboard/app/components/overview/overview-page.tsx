import { useCrons, useOverviewRuns } from "@/lib/queries";
import { ACTIVE_STATUSES } from "@/lib/types";
import { ActiveCrons } from "@/components/overview/active-crons";
import { RecentRuns } from "@/components/overview/recent-runs";
import { StatTile } from "@/components/overview/stat-tile";
import { StatusBar } from "@/components/overview/status-bar";
import { Section } from "@/components/ui/card";

export const Overview = (_props: { path?: string; default?: boolean }) => {
  const { data: runsPage } = useOverviewRuns();
  const { data: cronsData } = useCrons();
  const rs = runsPage?.runs ?? [];
  const count = (pred: (s: string) => boolean) => rs.filter((r) => pred(r.status)).length;
  return (
    <div class="flex flex-col gap-3">
      <div class="grid grid-cols-2 gap-3 sm:grid-cols-4">
        <StatTile label="Runs" value={rs.length} />
        <StatTile
          label="Active"
          value={count((s) => ACTIVE_STATUSES.has(s as never))}
          tone="running"
        />
        <StatTile label="Failed" value={count((s) => s === "failed")} tone="failed" />
        <StatTile label="Done" value={count((s) => s === "done")} tone="done" />
      </div>
      <Section title="Status distribution">
        <StatusBar runs={rs} />
      </Section>
      <RecentRuns runs={rs} />
      <ActiveCrons crons={cronsData?.crons ?? []} />
    </div>
  );
};
