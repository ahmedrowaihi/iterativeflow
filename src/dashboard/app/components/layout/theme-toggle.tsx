import { theme, toggleTheme } from "@/lib/theme";
import { Button } from "@/components/ui/button";
import { Moon, Sun } from "@/components/ui/icon";

export const ThemeToggle = () => (
  <Button
    variant="ghost"
    size="xs"
    onClick={toggleTheme}
    title={theme.value === "dark" ? "Switch to light" : "Switch to dark"}
  >
    {theme.value === "dark" ? <Sun /> : <Moon />}
  </Button>
);
