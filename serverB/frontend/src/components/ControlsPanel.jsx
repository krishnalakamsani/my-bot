import React, { useContext, useState } from "react";
import { AppContext } from "@/App";
import { Play, Square, XCircle, RefreshCw } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Switch } from "@/components/ui/switch";
import { Label } from "@/components/ui/label";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";

const ControlsPanel = () => {
  const { 
    botStatus, 
    position, 
    config, 
    indices, 
    timeframes, 
    startBot, 
    stopBot, 
    squareOff, 
    updateConfig,
    setMode, 
    setSelectedIndex, 
    setTimeframe 
  } = useContext(AppContext);
  
  const [loading, setLoading] = useState({
    start: false,
    stop: false,
    squareoff: false,
    tradingEnabled: false,
  });

  const handleStart = async () => {
    setLoading((prev) => ({ ...prev, start: true }));
    await startBot();
    setLoading((prev) => ({ ...prev, start: false }));
  };

  const handleStop = async () => {
    setLoading((prev) => ({ ...prev, stop: true }));
    await stopBot();
    setLoading((prev) => ({ ...prev, stop: false }));
  };

  const handleSquareOff = async () => {
    setLoading((prev) => ({ ...prev, squareoff: true }));
    await squareOff();
    setLoading((prev) => ({ ...prev, squareoff: false }));
  };

  const handleModeChange = async (checked) => {
    await setMode(checked ? "live" : "paper");
  };

  const handleTradingEnabledChange = async (checked) => {
    setLoading((prev) => ({ ...prev, tradingEnabled: true }));
    await updateConfig({ trading_enabled: checked });
    setLoading((prev) => ({ ...prev, tradingEnabled: false }));
  };

  const handleIndexChange = async (value) => {
    await setSelectedIndex(value);
  };

  const handleTimeframeChange = async (value) => {
    await setTimeframe(parseInt(value));
  };

  const canChangeMode = !position?.has_position;
  const canChangeSettings = !botStatus.is_running && !position?.has_position;

  // Get selected index info
  const selectedIndexInfo = indices.find(i => i.name === (config.selected_index || "NIFTY")) || {};
  const getExpiryLabel = (index) => {
    if (!index.expiry_type) return "";
    const days = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"];
    if (index.expiry_type === "weekly") {
      return `Weekly (${days[index.expiry_day]})`;
    } else {
      return `Monthly (Last ${days[index.expiry_day]})`;
    }
  };

  return (
    <div className="terminal-card" data-testid="controls-panel">
      <div className="terminal-card-header">
        <h2 className="text-sm font-semibold text-gray-900 font-[Manrope]">
          Controls
        </h2>
      </div>

      <div className="p-4 space-y-4">
        {/* Index Selection */}
        <div className="space-y-2">
          <Label className="text-xs font-medium text-gray-600">Index</Label>
          <Select
            value={config.selected_index || "NIFTY"}
            onValueChange={handleIndexChange}
            disabled={!canChangeSettings}
          >
            <SelectTrigger className="w-full rounded-sm" data-testid="index-select">
              <SelectValue placeholder="Select Index" />
            </SelectTrigger>
            <SelectContent>
              {indices.map((index) => (
                <SelectItem key={index.name} value={index.name}>
                  <div className="flex items-center justify-between w-full">
                    <span>{index.name}</span>
                    <span className="text-xs text-gray-400 ml-2">
                      Lot: {index.lot_size}
                    </span>
                  </div>
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
          {selectedIndexInfo.expiry_type && (
            <p className="text-xs text-gray-500">
              Expiry: {getExpiryLabel(selectedIndexInfo)}
            </p>
          )}
        </div>

        {/* Timeframe Selection */}
        <div className="space-y-2">
          <Label className="text-xs font-medium text-gray-600">Timeframe</Label>
          <Select
            value={String(config.candle_interval || 5)}
            onValueChange={handleTimeframeChange}
            disabled={!canChangeSettings}
          >
            <SelectTrigger className="w-full rounded-sm" data-testid="timeframe-select">
              <SelectValue placeholder="Select Timeframe" />
            </SelectTrigger>
            <SelectContent>
              {timeframes.map((tf) => (
                <SelectItem key={tf.value} value={String(tf.value)}>
                  {tf.label}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
          {!canChangeSettings && (
            <p className="text-xs text-amber-600">Stop bot to change settings</p>
          )}
        </div>

        {/* Start/Stop Buttons */}
        <div className="grid grid-cols-2 gap-2">
          <Button
            onClick={handleStart}
            disabled={botStatus.is_running || loading.start}
            className="w-full h-10 bg-emerald-600 hover:bg-emerald-700 text-white rounded-sm btn-active"
            data-testid="start-bot-btn"
          >
            {loading.start ? (
              <RefreshCw className="w-4 h-4 mr-2 animate-spin" />
            ) : (
              <Play className="w-4 h-4 mr-2" />
            )}
            Start
          </Button>

          <Button
            onClick={handleStop}
            disabled={!botStatus.is_running || loading.stop}
            variant="outline"
            className="w-full h-10 rounded-sm btn-active border-gray-300"
            data-testid="stop-bot-btn"
          >
            {loading.stop ? (
              <RefreshCw className="w-4 h-4 mr-2 animate-spin" />
            ) : (
              <Square className="w-4 h-4 mr-2" />
            )}
            Stop
          </Button>
        </div>

        {/* Square Off Button */}
        <Button
          disabled={!position?.has_position || loading.squareoff}
          variant="destructive"
          className="w-full h-10 rounded-sm btn-active"
          data-testid="squareoff-btn"
          onClick={handleSquareOff}
        >
          {loading.squareoff ? (
            <RefreshCw className="w-4 h-4 mr-2 animate-spin" />
          ) : (
            <XCircle className="w-4 h-4 mr-2" />
          )}
          Square Off Now
        </Button>

        {/* Mode Toggle */}
        <div className="flex items-center justify-between p-3 bg-gray-50 rounded-sm border border-gray-100">
          <div>
            <Label htmlFor="mode-toggle" className="text-sm font-medium">
              Trading Mode
            </Label>
            <p className="text-xs text-gray-500">
              {canChangeMode
                ? "Switch between paper and live"
                : "Close position to change"}
            </p>
          </div>
          <div className="flex items-center gap-2">
            <span
              className={`text-xs font-medium ${
                botStatus.mode === "paper" ? "text-blue-600" : "text-gray-400"
              }`}
            >
              Paper
            </span>
            <Switch
              id="mode-toggle"
              checked={botStatus.mode === "live"}
              onCheckedChange={handleModeChange}
              disabled={!canChangeMode}
              data-testid="mode-toggle"
            />
            <span
              className={`text-xs font-medium ${
                botStatus.mode === "live" ? "text-amber-600" : "text-gray-400"
              }`}
            >
              Live
            </span>
          </div>
        </div>

        {/* Trading Enable Toggle (Pause Entries) */}
        <div className="flex items-center justify-between p-3 bg-gray-50 rounded-sm border border-gray-100">
          <div>
            <Label htmlFor="trading-enabled-toggle" className="text-sm font-medium">
              Take New Trades
            </Label>
            <p className="text-xs text-gray-500">
              {config?.trading_enabled === false
                ? "Paused: indicators run, no new entries"
                : "Enabled: bot can enter on signals"}
            </p>
          </div>
          <div className="flex items-center gap-2">
            <span className="text-xs font-medium text-gray-500">Off</span>
            <Switch
              id="trading-enabled-toggle"
              checked={config?.trading_enabled !== false}
              onCheckedChange={handleTradingEnabledChange}
              disabled={loading.tradingEnabled}
              data-testid="trading-enabled-toggle"
            />
            <span className="text-xs font-medium text-emerald-700">On</span>
          </div>
        </div>
      </div>
    </div>
  );
};

export default ControlsPanel;
