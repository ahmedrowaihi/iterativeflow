import { render } from "preact";
import "franken-ui";
import { SWRConfig } from "swr";
import { App } from "@/App";
import { autoRefresh } from "@/lib/store";
import "@/styles.css";

const Root = () => (
  <SWRConfig value={{ refreshInterval: autoRefresh.value ? 5000 : 0, keepPreviousData: true }}>
    <App />
  </SWRConfig>
);

const root = document.getElementById("app");
if (root) render(<Root />, root);
