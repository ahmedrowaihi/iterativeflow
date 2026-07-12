import { highlight } from "sugar-high";
import { CopyButton } from "@/components/common/copy-button";

export const Pre = ({ code }: { code: string }) => (
  <div class="relative">
    <CopyButton text={code} class="absolute top-1.5 right-1.5" />
    <pre
      class="m-0 max-h-72 overflow-auto rounded-md bg-muted p-3 pr-10 font-mono text-xs"
      dangerouslySetInnerHTML={{ __html: highlight(code) }}
    />
  </div>
);
