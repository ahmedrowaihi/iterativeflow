import type { JSX } from "preact";
import { closeJson, jsonView } from "@/lib/store";
import { JsonPreview } from "@/components/common/json-preview";
import { Button } from "@/components/ui/button";
import { Close } from "@/components/ui/icon";

export const JsonDialog = () => {
  const v = jsonView.value;
  if (!v) return null;
  return (
    <div
      class="fixed inset-0 z-50 flex items-center justify-center bg-black/50 p-4"
      onClick={(e: JSX.TargetedMouseEvent<HTMLDivElement>) => {
        if (e.target === e.currentTarget) closeJson();
      }}
    >
      <div class="flex max-h-[80vh] w-full max-w-2xl flex-col overflow-hidden rounded-md border border-border bg-card">
        <div class="flex items-center gap-2 border-b border-border px-4 py-2.5">
          <h2 class="font-mono text-sm font-semibold">{v.title}</h2>
          <span class="flex-1" />
          <Button variant="ghost" size="xs" onClick={closeJson} title="Close">
            <Close />
          </Button>
        </div>
        <div class="overflow-auto p-4">
          <JsonPreview capped={v.capped} />
        </div>
      </div>
    </div>
  );
};
