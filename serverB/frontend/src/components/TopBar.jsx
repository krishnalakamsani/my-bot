import React, { useContext } from "react";
import { useNavigate } from "react-router-dom";
import { AppContext } from "@/App";
import { Settings, Wifi, WifiOff, Clock, TrendingUp, BarChart3 } from "lucide-react";
import { Button } from "@/components/ui/button";

const TopBar = ({ onSettingsClick }) => {
  const { botStatus, wsConnected, config } = useContext(AppContext);
  const navigate = useNavigate();

  // Format timeframe for display
  const formatTimeframe = (seconds) => {
    if (!seconds) return "5s";
    if (seconds < 60) return `${seconds}s`;
    if (seconds < 3600) return `${seconds / 60}m`;
    return `${seconds / 3600}h`;
  };

  return (
    <div
      className="bg-white border-b border-gray-200 px-4 lg:px-6 py-3 flex items-center justify-between"
      data-testid="top-bar"
    >
      {/* Left - Logo & Title */}
      <div className="flex items-center gap-3">
        <div className="w-8 h-8 bg-gradient-to-br from-blue-600 to-indigo-700 rounded-sm flex items-center justify-center">
          <TrendingUp className="w-4 h-4 text-white" />
        </div>
        <div>
          <h1 className="text-base font-semibold text-gray-900 font-[Manrope] tracking-tight">
            SuperTrend Bot
          </h1>
          <p className="text-xs text-gray-500">
            {config.selected_index || "NIFTY"} Options Trading
          </p>
        </div>
      </div>

      {/* Center - Status Indicators */}
      <div className="hidden md:flex items-center gap-4">
        {/* Index Badge */}
        <div className="flex items-center gap-1.5 px-2.5 py-1 bg-blue-50 rounded-sm">
          <span className="text-xs font-medium text-blue-700">
            {config.selected_index || "NIFTY"}
          </span>
        </div>

        {/* Timeframe Badge */}
        <div className="flex items-center gap-1.5 px-2.5 py-1 bg-purple-50 rounded-sm">
          <Clock className="w-3 h-3 text-purple-600" />
          <span className="text-xs font-medium text-purple-700">
            {formatTimeframe(botStatus.candle_interval || config.candle_interval)}
          </span>
        </div>

        {/* Bot Status */}
        <div
          className={`status-badge ${
            botStatus.is_running ? "status-running" : "status-stopped"
          }`}
          data-testid="bot-status-badge"
        >
          <span
            className={`w-1.5 h-1.5 rounded-full ${
              botStatus.is_running ? "bg-emerald-500" : "bg-gray-400"
            }`}
          />
          {botStatus.is_running ? "Running" : "Stopped"}
        </div>

        {/* Trading Paused Indicator */}
        {botStatus.is_running && botStatus.trading_enabled === false && (
          <div
            className="status-badge status-warning"
            data-testid="trading-paused-badge"
            title="Entries paused: bot keeps updating indicators/prices"
          >
            PAUSED
          </div>
        )}

        {/* Mode Status */}
        <div
          className={`status-badge ${
            botStatus.mode === "live" ? "status-warning" : "status-info"
          }`}
          data-testid="mode-badge"
        >
          {botStatus.mode === "live" ? "LIVE" : "PAPER"}
        </div>

        {/* Market Status - PROMINENT */}
        <div
          className={`status-badge ${
            botStatus.market_status === "open" ? "status-running" : "status-error"
          }`}
          data-testid="market-status-badge"
          title={botStatus.market_details ? `${botStatus.market_details.current_time_ist} IST` : ""}
        >
          <span className={`w-2 h-2 rounded-full ${
            botStatus.market_status === "open" ? "bg-emerald-500 animate-pulse" : "bg-red-500"
          }`} />
          <span className="font-semibold">
            {botStatus.market_status === "open" ? "🟢 OPEN" : "🔴 CLOSED"}
          </span>
          {botStatus.market_details && (
            <span className="text-xs opacity-70 ml-1">
              {botStatus.market_details.current_time_ist}
            </span>
          )}
        </div>

        {/* Connection Status */}
        <div
          className={`status-badge ${
            wsConnected ? "status-running" : "status-error"
          }`}
          data-testid="ws-status-badge"
        >
          {wsConnected ? (
            <Wifi className="w-3 h-3" />
          ) : (
            <WifiOff className="w-3 h-3" />
          )}
          {wsConnected ? "Connected" : "Disconnected"}
        </div>
      </div>

      {/* Right - Settings & Analysis Buttons */}
      <div className="flex items-center gap-2">
        <Button
          variant="outline"
          size="sm"
          onClick={() => navigate('/analysis')}
          className="rounded-sm btn-active hidden sm:flex"
          data-testid="analysis-btn"
          title="View trade analysis and statistics"
        >
          <BarChart3 className="w-4 h-4 mr-1" />
          Analysis
        </Button>
        <Button
          variant="outline"
          size="sm"
          onClick={onSettingsClick}
          className="rounded-sm btn-active"
          data-testid="settings-btn"
        >
          <Settings className="w-4 h-4 mr-1" />
          Settings
        </Button>
      </div>
    </div>
  );
};

export default TopBar;
