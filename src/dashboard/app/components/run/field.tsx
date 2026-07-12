import type { ComponentChildren } from "preact";

export const Field = ({ label, children }: { label: string; children: ComponentChildren }) => (
  <div class="flex min-w-0 flex-col gap-1">
    <dt class="text-xs text-muted-foreground">{label}</dt>
    <dd class="min-w-0 wrap-break-word text-sm">{children}</dd>
  </div>
);
