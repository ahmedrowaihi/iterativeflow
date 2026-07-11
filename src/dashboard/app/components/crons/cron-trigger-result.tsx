import { fmtAbs } from "@/lib/format";
import { cronResults } from "@/lib/store";
import { JsonPreview } from "@/components/common/json-preview";
import { Section } from "@/components/ui/card";

export const CronTriggerResult = ({ name }: { name: string }) => {
  const res = cronResults.value.get(name);
  if (!res) return null;
  return (
    <Section title="Last trigger">
      <div class="flex flex-col gap-2">
        <p class="text-xs text-muted-foreground">{fmtAbs(res.at)}</p>
        {res.error ? (
          <p class="text-xs text-destructive">error: {res.error}</p>
        ) : res.result ? (
          <JsonPreview capped={res.result} />
        ) : (
          <p class="text-xs text-muted-foreground">no return value</p>
        )}
      </div>
    </Section>
  );
};
