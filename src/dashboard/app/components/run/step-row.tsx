import { dur, fmtAbs, rel } from "@/lib/format";
import { expanded, toggleStep } from "@/lib/store";
import type { StepRow as StepData } from "@/lib/types";
import { JsonPreview } from "@/components/common/json-preview";
import { StatusBadge } from "@/components/common/status-badge";
import { Button } from "@/components/ui/button";
import { Chevron } from "@/components/ui/icon";
import { Td, Tr } from "@/components/ui/table";

export const StepRow = ({ step }: { step: StepData }) => {
  const isOpen = expanded.value.has(step.cursorKey);
  const hasBody = step.result || step.error;
  return (
    <>
      <Tr>
        <Td class="font-mono">{step.cursorKey}</Td>
        <Td>
          <StatusBadge status={step.status} />
        </Td>
        <Td>{step.attempts}</Td>
        <Td title={fmtAbs(step.startedAt)}>{rel(step.startedAt)}</Td>
        <Td>{step.completedAt ? dur(step.startedAt, step.completedAt) : "—"}</Td>
        <Td>
          {hasBody ? (
            <Button variant="ghost" size="xs" onClick={() => toggleStep(step.cursorKey)}>
              <Chevron open={isOpen} />
            </Button>
          ) : null}
        </Td>
      </Tr>
      {isOpen && hasBody ? (
        <Tr>
          <Td colSpan={6}>
            <div class="flex flex-col gap-3">
              {step.error ? (
                <p class="text-xs text-muted-foreground">
                  error: <span class="font-mono">{step.error.code}</span> {step.error.message}
                </p>
              ) : null}
              {step.result ? <JsonPreview capped={step.result} /> : null}
            </div>
          </Td>
        </Tr>
      ) : null}
    </>
  );
};
