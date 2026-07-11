import { useCallback } from "react";
import { showToast } from "@/lib/store";

export const useCopy = () =>
  useCallback((text: string, message = "copied") => {
    navigator.clipboard?.writeText(text).then(() => showToast(message));
  }, []);
