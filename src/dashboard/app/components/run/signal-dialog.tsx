import { useState } from "preact/hooks";
import { closeSignal, sendSignal, signalTarget } from "@/lib/store";
import { Button } from "@/components/ui/button";
import { Dialog, DialogActions, DialogTitle } from "@/components/ui/dialog";
import { Textarea } from "@/components/ui/input";

export const SignalDialog = () => {
  const target = signalTarget.value;
  if (!target) return null;
  return <SignalForm key={`${target.runId}:${target.name}`} target={target} />;
};

const SignalForm = ({ target }: { target: { runId: string; name: string } }) => {
  const [raw, setRaw] = useState("");
  const [error, setError] = useState<string | null>(null);

  const submit = () => {
    let payload: unknown;
    if (raw.trim()) {
      try {
        payload = JSON.parse(raw);
      } catch {
        setError("payload must be valid JSON");
        return;
      }
    }
    sendSignal(target.runId, target.name, payload);
  };

  return (
    <Dialog onDismiss={closeSignal}>
      <DialogTitle>Send signal · {target.name}</DialogTitle>
      <div class="mt-2 mb-4 flex flex-col gap-1.5">
        <label class="text-xs text-muted-foreground">Payload (JSON, optional)</label>
        <Textarea
          rows={6}
          placeholder={'{\n  "approvedBy": "u_9"\n}'}
          value={raw}
          onInput={(e) => {
            setRaw((e.target as HTMLTextAreaElement).value);
            setError(null);
          }}
        />
        {error ? <p class="text-xs text-destructive">{error}</p> : null}
      </div>
      <DialogActions>
        <Button onClick={closeSignal}>Cancel</Button>
        <Button variant="primary" onClick={submit}>
          Send signal
        </Button>
      </DialogActions>
    </Dialog>
  );
};
