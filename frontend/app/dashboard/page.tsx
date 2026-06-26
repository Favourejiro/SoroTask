"use client";

import WidgetGrid from "@/components/WidgetGrid";

type WidgetStatus = "loading" | "empty" | "error" | "success";
type WidgetSize = "small" | "medium" | "large";

type WidgetDefinition = {
  id: string;
  title: string;
  description: string;
  defaultSize: WidgetSize;
  getStatus: () => WidgetStatus;
  render: () => JSX.Element;
};

const widgetRegistry: Record<string, WidgetDefinition> = {
  volume: {
    id: "volume",
    title: "Daily Volume",
    description: "Track total processed volume over the last 24h.",
    defaultSize: "large",
    getStatus: () => "success",
    render: () => (
      <div className="space-y-3">
        <p className="text-3xl font-semibold text-slate-100">$2.41M</p>
        <p className="text-sm text-emerald-300">+12.3% vs yesterday</p>
      </div>
    ),
  },
  keeperHealth: {
    id: "keeperHealth",
    title: "Keeper Health",
    description: "Heartbeat and execution reliability overview.",
    defaultSize: "medium",
    getStatus: () => "success",
    render: () => (
      <div className="space-y-2 text-sm text-slate-200">
        <p>Healthy keepers: 9/10</p>
        <p>Failed executions: 0.8%</p>
      </div>
    ),
  },
  failedTasks: {
    id: "failedTasks",
    title: "Failed Tasks",
    description: "Tasks requiring intervention.",
    defaultSize: "small",
    getStatus: () => "empty",
    render: () => (
      <p className="text-sm text-slate-300">No failed tasks detected.</p>
    ),
  },
  bridgeLatency: {
    id: "bridgeLatency",
    title: "Bridge Latency",
    description: "Cross-network median latency.",
    defaultSize: "small",
    getStatus: () => "loading",
    render: () => (
      <p className="text-sm text-slate-300">Measuring live latency...</p>
    ),
  },
  alertFeed: {
    id: "alertFeed",
    title: "Alert Feed",
    description: "Recent critical incidents and warnings.",
    defaultSize: "medium",
    getStatus: () => "error",
    render: () => (
      <p className="text-sm text-slate-300">
        Alert stream is temporarily unavailable. Retry shortly.
      </p>
    ),
  },
};

export default function DashboardPage() {
  return (
    <main className="mx-auto min-h-screen max-w-7xl px-4 py-8 sm:px-6 lg:px-8">
      <header data-onboarding="dashboard" className="mb-8 flex flex-col gap-2">
        <h1 className="text-3xl font-semibold text-slate-100">
          Analytics Dashboard
        </h1>
        <p className="text-sm text-slate-300">
          Drag cards to reorder them, or toggle widgets to personalize your workspace.
        </p>
      </header>

      <WidgetGrid widgetRegistry={widgetRegistry} />
    </main>
  );
}