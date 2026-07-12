import { openJson } from "@/lib/store";
import type { CappedJson } from "@/lib/types";
import { Button } from "@/components/ui/button";

export const JsonViewButton = ({ title, capped }: { title: string; capped: CappedJson }) => (
  <Button variant="default" size="xs" onClick={() => openJson(title, capped)}>
    View
  </Button>
);
