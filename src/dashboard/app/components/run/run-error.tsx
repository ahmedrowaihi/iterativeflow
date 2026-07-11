import type { FlowErrorLite } from "@/lib/types";
import { Alert } from "@/components/ui/alert";
import { Pre } from "@/components/ui/pre";

export const RunError = ({ error }: { error: FlowErrorLite }) => (
  <Alert variant="destructive">
    <div class="flex flex-col gap-3">
      <div class="font-semibold">
        <span class="font-mono">{error.code}</span> — {error.message}
      </div>
      {error.stack ? (
        <details class="flex flex-col gap-2">
          <summary class="cursor-pointer text-xs text-muted-foreground">stack</summary>
          <Pre code={error.stack} />
        </details>
      ) : null}
    </div>
  </Alert>
);
