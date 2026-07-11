import type { ComponentChildren, JSX } from "preact";
import { useEscape } from "@/lib/use-escape";
import { CardTitle } from "@/components/ui/card";

export const Dialog = ({
  onDismiss,
  children,
}: {
  onDismiss?: () => void;
  children: ComponentChildren;
}) => {
  useEscape(() => onDismiss?.());
  return (
    <div
      class="fixed inset-0 z-50 flex items-center justify-center bg-black/50 p-4"
      onClick={(e: JSX.TargetedMouseEvent<HTMLDivElement>) => {
        if (e.target === e.currentTarget) onDismiss?.();
      }}
    >
      <div class="uk-card uk-card-body w-full max-w-md" role="dialog" aria-modal="true">
        {children}
      </div>
    </div>
  );
};

export const DialogTitle = ({ children }: { children: ComponentChildren }) => (
  <CardTitle>{children}</CardTitle>
);

export const DialogBody = ({ children }: { children: ComponentChildren }) => (
  <p class="mt-2 mb-4 text-muted-foreground">{children}</p>
);

export const DialogActions = ({ children }: { children: ComponentChildren }) => (
  <div class="flex justify-end gap-2">{children}</div>
);
