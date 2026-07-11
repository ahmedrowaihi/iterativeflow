import { cn } from "@/lib/cn";
import { toastMsg } from "@/lib/store";

export const Toaster = () => {
  const msg = toastMsg.value;
  return (
    <div
      class={cn(
        "pointer-events-none fixed bottom-6 left-1/2 z-30 -translate-x-1/2 rounded-md bg-foreground px-4 py-2 text-sm text-background transition-opacity",
        msg ? "opacity-100" : "opacity-0",
      )}
    >
      {msg}
    </div>
  );
};
