import type { CappedJson } from "@/lib/types";
import { Pre } from "@/components/ui/pre";

export const JsonPreview = ({ capped }: { capped: CappedJson }) => (
  <div class="flex flex-col gap-2">
    <Pre code={capped.preview} />
    {capped.truncated ? (
      <p class="text-xs text-muted-foreground">
        truncated preview — {capped.size.toLocaleString()} chars total
      </p>
    ) : null}
  </div>
);
