import type { RunDetail } from "@/lib/types";
import { JsonPreview } from "@/components/common/json-preview";
import { RunError } from "@/components/run/run-error";
import { RunOverview } from "@/components/run/run-overview";
import { SignalsTable } from "@/components/run/signals-table";
import { SleepsTable } from "@/components/run/sleeps-table";
import { StepsTable } from "@/components/run/steps-table";
import { Section } from "@/components/ui/card";
import { Empty } from "@/components/ui/empty";

export const RunBody = ({ detail, error }: { detail?: RunDetail; error?: unknown }) => {
  if (error) return <Empty>Failed to load run.</Empty>;
  if (!detail) return <Empty>Loading…</Empty>;
  const { run, steps, timers, signals } = detail;
  return (
    <div class="flex flex-col gap-3 p-4">
      <RunOverview run={run} />
      {run.error ? <RunError error={run.error} /> : null}
      {run.input ? (
        <Section title="Input">
          <JsonPreview capped={run.input} />
        </Section>
      ) : null}
      {run.output ? (
        <Section title="Output">
          <JsonPreview capped={run.output} />
        </Section>
      ) : null}
      <StepsTable steps={steps} />
      {timers.length ? <SleepsTable timers={timers} /> : null}
      {signals.length ? <SignalsTable runId={run.id} signals={signals} /> : null}
    </div>
  );
};
