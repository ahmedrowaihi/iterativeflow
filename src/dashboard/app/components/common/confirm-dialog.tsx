import { answerConfirm, confirmState } from "@/lib/store";
import { Button } from "@/components/ui/button";
import { Dialog, DialogActions, DialogBody, DialogTitle } from "@/components/ui/dialog";

export const ConfirmDialog = () => {
  const c = confirmState.value;
  if (!c) return null;
  return (
    <Dialog onDismiss={() => answerConfirm(false)}>
      <DialogTitle>{c.title}</DialogTitle>
      <DialogBody>{c.message}</DialogBody>
      <DialogActions>
        <Button onClick={() => answerConfirm(false)}>Keep it</Button>
        <Button variant={c.danger ? "destructive" : "primary"} onClick={() => answerConfirm(true)}>
          {c.title}
        </Button>
      </DialogActions>
    </Dialog>
  );
};
