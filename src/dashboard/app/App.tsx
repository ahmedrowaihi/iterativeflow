import Router from "preact-router";
import { currentPath, history } from "@/lib/router";
import { ConfirmDialog } from "@/components/common/confirm-dialog";
import { CronSheet } from "@/components/crons/cron-sheet";
import { CronsPage } from "@/components/crons/crons-page";
import { Sidebar } from "@/components/layout/sidebar";
import { TopBar } from "@/components/layout/top-bar";
import { Overview } from "@/components/overview/overview-page";
import { RunSheet } from "@/components/run/run-sheet";
import { SignalDialog } from "@/components/run/signal-dialog";
import { RunsPage } from "@/components/runs/runs-page";
import { JsonDialog } from "@/components/ui/json-dialog";
import { Toaster } from "@/components/ui/toaster";

export const App = () => (
  <>
    <div class="flex h-screen">
      <Sidebar />
      <div class="flex min-w-0 flex-1 flex-col">
        <TopBar />
        <main class="flex-1 overflow-y-auto p-4">
          <Router history={history} url={currentPath()}>
            <Overview path="/" default />
            <RunsPage path="/runs" />
            <CronsPage path="/crons" />
          </Router>
        </main>
      </div>
    </div>
    <CronSheet />
    <RunSheet />
    <SignalDialog />
    <JsonDialog />
    <ConfirmDialog />
    <Toaster />
  </>
);
