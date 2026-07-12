import { useCopy } from "@/lib/use-copy";
import { Button } from "@/components/ui/button";
import { Copy } from "@/components/ui/icon";

export const CopyButton = ({ text, class: cls }: { text: string; class?: string }) => {
  const copy = useCopy();
  return (
    <Button variant="ghost" size="xs" class={cls} title="Copy" onClick={() => copy(text)}>
      <Copy />
    </Button>
  );
};
