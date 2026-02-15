import React, { useContext } from "react";
import { useNavigate } from "react-router-dom";
import { AppContext } from "@/App";
import TopBar from "@/components/TopBar";
import PositionPanel from "@/components/PositionPanel";
import ControlsPanel from "@/components/ControlsPanel";
import NiftyTracker from "@/components/NiftyTracker";
import TradesTable from "@/components/TradesTable";
import DailySummary from "@/components/DailySummary";
import LogsViewer from "@/components/LogsViewer";

const Dashboard = () => {
  const navigate = useNavigate();
  const context = useContext(AppContext);

  if (!context) {
    return <div>Loading...</div>;
  }

  return (
    <div className="h-screen flex flex-col bg-white" data-testid="dashboard">
      {/* Top Bar */}
      <TopBar onSettingsClick={() => navigate("/settings")} />

      {/* Main Content */}
      <div className="flex-1 overflow-auto p-4 lg:p-6">
        <div className="bento-grid h-full">
          {/* Left Column - Position & Controls (3 cols) */}
          <div className="col-span-12 lg:col-span-3 flex flex-col gap-4">
            <PositionPanel />
            <ControlsPanel />
          </div>

          {/* Middle Column - Nifty Tracker & Trades (6 cols) */}
          <div className="col-span-12 lg:col-span-6 flex flex-col gap-4">
            <NiftyTracker />
            <TradesTable />
          </div>

          {/* Right Column - Summary, Logs, Settings (3 cols) */}
          <div className="col-span-12 lg:col-span-3 flex flex-col gap-4">
            <DailySummary />
            <LogsViewer />
          </div>
        </div>
      </div>
    </div>
  );
};

export default Dashboard;
