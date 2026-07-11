import { fmtAbs, rel, shortId } from "@/lib/format";
import { runDuration } from "@/lib/run";
import { openRun } from "@/lib/store";
import type { RunFull } from "@/lib/types";
import { CopyButton } from "@/components/common/copy-button";
import { Field } from "@/components/run/field";
import { Badge } from "@/components/ui/badge";
import { Section } from "@/components/ui/card";

export const RunOverview = ({ run }: { run: RunFull }) => (
  <Section title="Overview">
    <dl class="grid grid-cols-2 gap-3 sm:grid-cols-3">
      <Field label="Run id">
        <div class="flex items-center gap-1.5">
          <span class="font-mono">{run.id}</span>
          <CopyButton text={run.id} />
        </div>
      </Field>
      <Field label="Created">
        <span title={fmtAbs(run.createdAt)}>{rel(run.createdAt)}</span>
      </Field>
      <Field label="Started">
        <span title={fmtAbs(run.startedAt)}>{run.startedAt ? rel(run.startedAt) : "—"}</span>
      </Field>
      <Field label="Completed">
        <span title={fmtAbs(run.completedAt)}>{run.completedAt ? rel(run.completedAt) : "—"}</span>
      </Field>
      <Field label="Duration">{runDuration(run)}</Field>
      <Field label="Attempts">{run.attempts}</Field>
      {run.parentRunId ? (
        <Field label="Parent run">
          <button
            type="button"
            class="cursor-pointer font-mono underline-offset-2 hover:underline"
            onClick={() => run.parentRunId && openRun(run.parentRunId)}
          >
            {shortId(run.parentRunId)}
          </button>
        </Field>
      ) : null}
      {run.idempotencyKey ? (
        <Field label="Idempotency key">
          <div class="flex items-center gap-1.5">
            <span class="font-mono">{run.idempotencyKey}</span>
            <CopyButton text={run.idempotencyKey} />
          </div>
        </Field>
      ) : null}
      {(run.tags ?? []).length ? (
        <Field label="Tags">
          <div class="flex flex-wrap gap-1.5">
            {(run.tags ?? []).map((t) => (
              <Badge variant="secondary">{t}</Badge>
            ))}
          </div>
        </Field>
      ) : null}
    </dl>
  </Section>
);
