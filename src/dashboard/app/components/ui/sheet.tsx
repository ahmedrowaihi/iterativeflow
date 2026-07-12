import type { ComponentChildren } from "preact";
import { useEscape } from "@/lib/use-escape";

export const Sheet = ({
  onClose,
  children,
}: {
  onClose: () => void;
  children: ComponentChildren;
}) => {
  useEscape(onClose);
  return (
    <>
      <div class="fixed inset-0 z-30 bg-black/50" onClick={onClose} />
      <div class="fixed inset-y-0 right-0 z-40 flex w-full max-w-4xl flex-col overflow-y-auto border-l border-border bg-background">
        {children}
      </div>
    </>
  );
};
