import React, { useContext, useEffect, useState } from "react";
import { useNavigate } from "react-router-dom";
import axios from "axios";
import { toast } from "sonner";
import { API, AppContext } from "@/App";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Switch } from "@/components/ui/switch";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Key, ShieldCheck, Eye, EyeOff, Save, ArrowLeft } from "lucide-react";

const Settings = () => {
  const navigate = useNavigate();
  const { config, updateConfig, botStatus, position } = useContext(AppContext);

  const canChangeRunContext = !botStatus?.is_running && !position?.has_position;
  const [bypassMarketHoursUpdating, setBypassMarketHoursUpdating] = useState(false);

  // API Credentials
  const [accessToken, setAccessToken] = useState("");
  const [clientId, setClientId] = useState("");
  const [showToken, setShowToken] = useState(false);

  // Risk Parameters
  const [orderQty, setOrderQty] = useState(config.order_qty);
  const [maxTrades, setMaxTrades] = useState(config.max_trades_per_day);
  const [maxLoss, setMaxLoss] = useState(config.daily_max_loss);
  const [maxLossPerTrade, setMaxLossPerTrade] = useState(config.max_loss_per_trade || 0);
  const [initialSL, setInitialSL] = useState(config.initial_stoploss || 0);
  const [trailStart, setTrailStart] = useState(config.trail_start_profit);
  const [trailStep, setTrailStep] = useState(config.trail_step);
  const [targetPoints, setTargetPoints] = useState(config.target_points || 0);
  const [riskPerTrade, setRiskPerTrade] = useState(config.risk_per_trade || 0);

  // Strategy Parameters
  const [indicatorType, setIndicatorType] = useState(config.indicator_type || "score_mds");
  // SuperTrend parameters are managed internally by the Score Engine; UI inputs removed.
  const [macdFast, setMacdFast] = useState(config.macd_fast || 12);
  const [macdSlow, setMacdSlow] = useState(config.macd_slow || 26);
  const [macdSignal, setMacdSignal] = useState(config.macd_signal || 9);
  const [macdConfirmationEnabled, setMacdConfirmationEnabled] = useState(
    config.macd_confirmation_enabled !== false
  );

  const [adxPeriod, setAdxPeriod] = useState(config.adx_period || 14);
  const [adxThreshold, setAdxThreshold] = useState(config.adx_threshold || 25);

  const [minTradeGap, setMinTradeGap] = useState(config.min_trade_gap || 0);
  const [tradeOnlyOnFlip, setTradeOnlyOnFlip] = useState(config.trade_only_on_flip !== false);

  const [htfFilterEnabled, setHtfFilterEnabled] = useState(config.htf_filter_enabled !== false);
  const [htfFilterTimeframe, setHtfFilterTimeframe] = useState(config.htf_filter_timeframe || 60);

  const [minHoldSeconds, setMinHoldSeconds] = useState(config.min_hold_seconds || 15);
  const [minOrderCooldownSeconds, setMinOrderCooldownSeconds] = useState(
    config.min_order_cooldown_seconds || 15
  );

  const normalizedIndicatorType = String(indicatorType || "").trim().toLowerCase();
  const indicatorUsesMacd = normalizedIndicatorType === "score_mds";
  const showFlipAndHtfControls = false;
  const indicatorUsesAdx = false;

  // Paper replay (testing)
  const [paperReplayEnabled, setPaperReplayEnabled] = useState(!!config.paper_replay_enabled);
  const [paperReplayDateIst, setPaperReplayDateIst] = useState(config.paper_replay_date_ist || "");

  // Saved strategies
  const [strategies, setStrategies] = useState([]);
  const [strategyName, setStrategyName] = useState("");
  const [selectedStrategyId, setSelectedStrategyId] = useState("");
  const [strategiesLoading, setStrategiesLoading] = useState(false);
  const fileInputRef = React.useRef(null);

  const [saving, setSaving] = useState(false);
  const isFirstRender = React.useRef(true);

  // Get index info
  const indexInfo = {
    NIFTY: { lot_size: 50, strike_interval: 100 },
    BANKNIFTY: { lot_size: 15, strike_interval: 100 },
    FINNIFTY: { lot_size: 40, strike_interval: 50 },
    MIDCPNIFTY: { lot_size: 75, strike_interval: 50 },
  };
  const currentIndexInfo = indexInfo[config?.selected_index] || { lot_size: 50, strike_interval: 100 };

  React.useEffect(() => {
    if (isFirstRender.current) {
      setOrderQty(config?.order_qty || 1);
      setMaxTrades(config?.max_trades_per_day || 5);
      setMaxLoss(config?.daily_max_loss || 2000);
      setMaxLossPerTrade(config?.max_loss_per_trade || 0);
      setInitialSL(config?.initial_stoploss || 50);
      setTrailStart(config?.trail_start_profit || 10);
      setTrailStep(config?.trail_step || 5);
      setTargetPoints(config?.target_points || 0);
      setRiskPerTrade(config?.risk_per_trade || 0);

      setIndicatorType(config?.indicator_type || "score_mds");
      // SuperTrend params are preserved from backend config, UI not editable here.
      setMacdFast(config?.macd_fast || 12);
      setMacdSlow(config?.macd_slow || 26);
      setMacdSignal(config?.macd_signal || 9);
      setMacdConfirmationEnabled(config?.macd_confirmation_enabled !== false);

      setAdxPeriod(config?.adx_period || 14);
      setAdxThreshold(config?.adx_threshold || 25);
      setMinTradeGap(config?.min_trade_gap || 0);
      setTradeOnlyOnFlip(config?.trade_only_on_flip !== false);
      setHtfFilterEnabled(config?.htf_filter_enabled !== false);
      setHtfFilterTimeframe(config?.htf_filter_timeframe || 60);
      setMinHoldSeconds(config?.min_hold_seconds || 15);
      setMinOrderCooldownSeconds(config?.min_order_cooldown_seconds || 15);

      setPaperReplayEnabled(!!config?.paper_replay_enabled);
      setPaperReplayDateIst(String(config?.paper_replay_date_ist || ""));

      isFirstRender.current = false;
    }
  }, []);

  const handleSaveCredentials = async () => {
    if (!accessToken || !clientId) {
      return;
    }
    setSaving(true);
    await updateConfig({
      dhan_access_token: accessToken,
      dhan_client_id: clientId,
    });
    setAccessToken("");
    setClientId("");
    setSaving(false);
  };

  const handleSaveRiskParams = async () => {
    setSaving(true);
    await updateConfig({
      order_qty: orderQty,
      max_trades_per_day: maxTrades,
      daily_max_loss: maxLoss,
      max_loss_per_trade: maxLossPerTrade,
      initial_stoploss: initialSL,
      trail_start_profit: trailStart,
      trail_step: trailStep,
      target_points: targetPoints,
      risk_per_trade: riskPerTrade,
    });
    setSaving(false);
  };

  const handleSaveStrategyParams = async () => {
    setSaving(true);
    await updateConfig({
      indicator_type: indicatorType,
      // Keep backend SuperTrend params unchanged when saving from UI
      supertrend_period: config?.supertrend_period,
      supertrend_multiplier: config?.supertrend_multiplier,
      macd_fast: macdFast,
      macd_slow: macdSlow,
      macd_signal: macdSignal,

      adx_period: adxPeriod,
      adx_threshold: adxThreshold,

      min_trade_gap: minTradeGap,
      trade_only_on_flip: tradeOnlyOnFlip,

      htf_filter_enabled: htfFilterEnabled,
      htf_filter_timeframe: htfFilterTimeframe,

      min_hold_seconds: minHoldSeconds,
      min_order_cooldown_seconds: minOrderCooldownSeconds,
    });
    setSaving(false);
  };

  const handleSaveReplayParams = async () => {
    if (paperReplayEnabled && !String(paperReplayDateIst || "").trim()) {
      toast.error("Select a replay date");
      return;
    }
    setSaving(true);
    await updateConfig({
      paper_replay_enabled: !!paperReplayEnabled,
      paper_replay_date_ist: String(paperReplayDateIst || ""),
    });
    setSaving(false);
  };

  const handleBypassMarketHoursChange = async (checked) => {
    if (!canChangeRunContext) return;
    setBypassMarketHoursUpdating(true);
    await updateConfig({ bypass_market_hours: !!checked });
    setBypassMarketHoursUpdating(false);
  };

  const buildStrategyConfig = () => {
    return {
      indicator_type: indicatorType,
      supertrend_period: supertrendPeriod,
      supertrend_multiplier: supertrendMultiplier,
      macd_fast: macdFast,
      macd_slow: macdSlow,
      macd_signal: macdSignal,
      macd_confirmation_enabled: macdConfirmationEnabled,

      adx_period: adxPeriod,
      adx_threshold: adxThreshold,

      min_trade_gap: minTradeGap,
      trade_only_on_flip: tradeOnlyOnFlip,

      htf_filter_enabled: htfFilterEnabled,
      htf_filter_timeframe: htfFilterTimeframe,

      min_hold_seconds: minHoldSeconds,
      min_order_cooldown_seconds: minOrderCooldownSeconds,

      // Include these so a strategy can fully define the run context
      selected_index: config?.selected_index,
      candle_interval: config?.candle_interval,
      order_qty: orderQty,

      // Risk & exit knobs
      max_trades_per_day: maxTrades,
      daily_max_loss: maxLoss,
      max_loss_per_trade: maxLossPerTrade,
      initial_stoploss: initialSL,
      trail_start_profit: trailStart,
      trail_step: trailStep,
      target_points: targetPoints,
      risk_per_trade: riskPerTrade,

      // Safety controls
      trading_enabled: config?.trading_enabled,
      bypass_market_hours: config?.bypass_market_hours,
    };
  };

  const fetchStrategies = async () => {
    setStrategiesLoading(true);
    try {
      const res = await axios.get(`${API}/strategies`);
      setStrategies(res.data || []);
    } catch (e) {
      toast.error(e.response?.data?.detail || "Failed to load strategies");
    } finally {
      setStrategiesLoading(false);
    }
  };

  useEffect(() => {
    fetchStrategies();
  }, []);

  const handleSaveStrategy = async () => {
    const name = String(strategyName || "").trim();
    if (!name) {
      toast.error("Enter a strategy name");
      return;
    }

    setStrategiesLoading(true);
    try {
      await axios.post(`${API}/strategies`, {
        name,
        config: buildStrategyConfig(),
      });
      toast.success("Strategy saved");
      setStrategyName("");
      await fetchStrategies();
    } catch (e) {
      toast.error(e.response?.data?.detail || "Failed to save strategy");
    } finally {
      setStrategiesLoading(false);
    }
  };

  const handleApplyAndRun = async () => {
    if (!selectedStrategyId) {
      toast.error("Select a strategy first");
      return;
    }

    setStrategiesLoading(true);
    try {
      const res = await axios.post(`${API}/strategies/${selectedStrategyId}/apply?start=true`);
      toast.success(res.data?.message || "Strategy applied");
    } catch (e) {
      toast.error(e.response?.data?.detail || "Failed to apply strategy");
    } finally {
      setStrategiesLoading(false);
    }
  };

  const handleApplyOnly = async () => {
    if (!selectedStrategyId) {
      toast.error("Select a strategy first");
      return;
    }

    setStrategiesLoading(true);
    try {
      const res = await axios.post(`${API}/strategies/${selectedStrategyId}/apply`);
      toast.success(res.data?.message || "Strategy applied");
    } catch (e) {
      toast.error(e.response?.data?.detail || "Failed to apply strategy");
    } finally {
      setStrategiesLoading(false);
    }
  };

  const canApplyStrategy = !botStatus?.is_running && !position?.has_position;

  const handleDeleteStrategy = async () => {
    if (!selectedStrategyId) {
      toast.error("Select a strategy first");
      return;
    }
    const selected = (strategies || []).find((s) => String(s.id) === String(selectedStrategyId));
    const ok = window.confirm(`Delete strategy '${selected?.name || ""}'?`);
    if (!ok) return;

    setStrategiesLoading(true);
    try {
      await axios.delete(`${API}/strategies/${selectedStrategyId}`);
      toast.success("Strategy deleted");
      setSelectedStrategyId("");
      await fetchStrategies();
    } catch (e) {
      toast.error(e.response?.data?.detail || "Failed to delete strategy");
    } finally {
      setStrategiesLoading(false);
    }
  };

  const handleRenameStrategy = async () => {
    if (!selectedStrategyId) {
      toast.error("Select a strategy first");
      return;
    }
    const name = String(strategyName || "").trim();
    if (!name) {
      toast.error("Enter a new name in Strategy Name");
      return;
    }
    setStrategiesLoading(true);
    try {
      await axios.patch(`${API}/strategies/${selectedStrategyId}`, { name });
      toast.success("Strategy renamed");
      setStrategyName("");
      await fetchStrategies();
    } catch (e) {
      toast.error(e.response?.data?.detail || "Failed to rename strategy");
    } finally {
      setStrategiesLoading(false);
    }
  };

  const handleDuplicateStrategy = async () => {
    if (!selectedStrategyId) {
      toast.error("Select a strategy first");
      return;
    }
    const name = String(strategyName || "").trim();
    if (!name) {
      toast.error("Enter a new name in Strategy Name");
      return;
    }
    setStrategiesLoading(true);
    try {
      await axios.post(`${API}/strategies/${selectedStrategyId}/duplicate`, { name });
      toast.success("Strategy duplicated");
      setStrategyName("");
      await fetchStrategies();
    } catch (e) {
      toast.error(e.response?.data?.detail || "Failed to duplicate strategy");
    } finally {
      setStrategiesLoading(false);
    }
  };

  const handleExportStrategies = async () => {
    setStrategiesLoading(true);
    try {
      const res = await axios.get(`${API}/strategies/export`);
      const payload = JSON.stringify(res.data || {}, null, 2);
      const blob = new Blob([payload], { type: "application/json" });
      const url = URL.createObjectURL(blob);
      const a = document.createElement("a");
      a.href = url;
      a.download = "strategies.json";
      document.body.appendChild(a);
      a.click();
      a.remove();
      URL.revokeObjectURL(url);
      toast.success("Export downloaded");
    } catch (e) {
      toast.error(e.response?.data?.detail || "Failed to export strategies");
    } finally {
      setStrategiesLoading(false);
    }
  };

  const handleImportClick = () => {
    fileInputRef.current?.click();
  };

  const handleImportFile = async (e) => {
    const file = e.target.files?.[0];
    e.target.value = "";
    if (!file) return;

    setStrategiesLoading(true);
    try {
      const text = await file.text();
      const parsed = JSON.parse(text);
      const strategiesList = parsed?.strategies;
      if (!Array.isArray(strategiesList)) {
        toast.error("Invalid file format: expected { strategies: [...] }");
        return;
      }
      const res = await axios.post(`${API}/strategies/import`, { strategies: strategiesList });
      toast.success(`Imported ${res.data?.imported ?? 0} strategies`);
      await fetchStrategies();
    } catch (err) {
      toast.error("Failed to import strategies");
    } finally {
      setStrategiesLoading(false);
    }
  };

  return (
    <div className="min-h-screen bg-gray-50">
      {/* Header */}
      <div className="bg-white border-b border-gray-200 p-4 lg:p-6">
        <div className="max-w-6xl mx-auto flex items-center gap-4">
          <Button
            onClick={() => navigate("/")}
            variant="ghost"
            size="sm"
            className="rounded-sm"
          >
            <ArrowLeft className="w-4 h-4 mr-2" />
            Back
          </Button>
          <h1 className="text-2xl font-bold">Settings</h1>
        </div>
      </div>

      {/* Content */}
      <div className="max-w-6xl mx-auto p-4 lg:p-6">
        <Tabs defaultValue="risk" className="w-full">
          <TabsList className="grid w-full grid-cols-3">
            <TabsTrigger value="credentials" className="text-xs">
              <Key className="w-3 h-3 mr-1" />
              API Keys
            </TabsTrigger>
            <TabsTrigger value="risk" className="text-xs">
              <ShieldCheck className="w-3 h-3 mr-1" />
              Risk
            </TabsTrigger>
            <TabsTrigger value="strategy" className="text-xs">
              Strategy
            </TabsTrigger>
          </TabsList>

          {/* API Credentials Tab */}
          <TabsContent value="credentials" className="space-y-4 mt-6 bg-white p-6 rounded-lg border border-gray-200">
            <div className="p-3 bg-amber-50 border border-amber-200 rounded-sm text-xs text-amber-800">
              <strong>Note:</strong> Dhan access token expires daily. Update it
              here each morning before trading.
            </div>

            <div className="space-y-3">
              <div>
                <Label htmlFor="client-id">Client ID</Label>
                <Input
                  id="client-id"
                  placeholder="Enter your Dhan Client ID"
                  value={clientId}
                  onChange={(e) => setClientId(e.target.value)}
                  className="mt-1 rounded-sm"
                  data-testid="client-id-input"
                />
              </div>

              <div>
                <Label htmlFor="access-token">Access Token</Label>
                <div className="relative mt-1">
                  <Input
                    id="access-token"
                    type={showToken ? "text" : "password"}
                    placeholder="Enter your Dhan Access Token"
                    value={accessToken}
                    onChange={(e) => setAccessToken(e.target.value)}
                    className="pr-10 rounded-sm"
                    data-testid="access-token-input"
                  />
                  <button
                    type="button"
                    onClick={() => setShowToken(!showToken)}
                    className="absolute right-3 top-1/2 -translate-y-1/2 text-gray-400 hover:text-gray-600"
                  >
                    {showToken ? (
                      <EyeOff className="w-4 h-4" />
                    ) : (
                      <Eye className="w-4 h-4" />
                    )}
                  </button>
                </div>
              </div>

              <div className="flex items-center justify-between pt-2">
                <span
                  className={`text-xs ${
                    config.has_credentials
                      ? "text-emerald-600"
                      : "text-amber-600"
                  }`}
                >
                  {config.has_credentials
                    ? "✓ Credentials configured"
                    : "⚠ No credentials set"}
                </span>
                <Button
                  onClick={handleSaveCredentials}
                  disabled={saving || !accessToken || !clientId}
                  size="sm"
                  className="rounded-sm btn-active"
                  data-testid="save-credentials-btn"
                >
                  <Save className="w-3 h-3 mr-1" />
                  {saving ? "Saving..." : "Save Credentials"}
                </Button>
              </div>
            </div>

            <div className="text-xs text-gray-500 pt-2 border-t border-gray-100">
              Get your credentials from{" "}
              <a
                href="https://web.dhan.co"
                target="_blank"
                rel="noopener noreferrer"
                className="text-blue-600 hover:underline"
              >
                web.dhan.co
              </a>{" "}
              → My Profile → DhanHQ Trading APIs
            </div>
          </TabsContent>

          {/* Risk Parameters Tab */}
          <TabsContent value="risk" className="space-y-4 mt-6 bg-white p-6 rounded-lg border border-gray-200">
            <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
              <div>
                <Label htmlFor="order-qty">Number of Lots</Label>
                <Input
                  id="order-qty"
                  type="number"
                  min="1"
                  max="10"
                  value={orderQty}
                  onChange={(e) => {
                    const val = parseInt(e.target.value) || 1;
                    setOrderQty(Math.min(10, Math.max(1, val)));
                  }}
                  className="mt-1 rounded-sm"
                  data-testid="order-qty-input"
                />
                <p className="text-xs text-gray-500 mt-1">
                  {orderQty} lot = {orderQty * currentIndexInfo.lot_size} qty
                </p>
              </div>

              <div>
                <Label htmlFor="max-trades">Max Trades/Day</Label>
                <Input
                  id="max-trades"
                  type="number"
                  value={maxTrades}
                  onChange={(e) => setMaxTrades(parseInt(e.target.value))}
                  className="mt-1 rounded-sm"
                  data-testid="max-trades-input"
                />
              </div>

              <div>
                <Label htmlFor="max-loss">Daily Max Loss (₹)</Label>
                <Input
                  id="max-loss"
                  type="number"
                  value={maxLoss}
                  onChange={(e) => setMaxLoss(parseFloat(e.target.value))}
                  className="mt-1 rounded-sm"
                  data-testid="max-loss-input"
                />
              </div>

              <div>
                <Label htmlFor="max-loss-per-trade">Max Loss Per Trade (₹)</Label>
                <Input
                  id="max-loss-per-trade"
                  type="number"
                  min="0"
                  value={maxLossPerTrade}
                  onChange={(e) => setMaxLossPerTrade(parseFloat(e.target.value))}
                  className="mt-1 rounded-sm"
                  data-testid="max-loss-per-trade-input"
                />
                <p className="text-xs text-gray-500 mt-1">0 = disabled</p>
              </div>

              <div>
                <Label htmlFor="initial-sl">Initial Stop Loss (points)</Label>
                <Input
                  id="initial-sl"
                  type="number"
                  min="0"
                  value={initialSL}
                  onChange={(e) => setInitialSL(parseFloat(e.target.value))}
                  className="mt-1 rounded-sm"
                  data-testid="initial-sl-input"
                />
                <p className="text-xs text-gray-500 mt-1">0 = disabled</p>
              </div>

              <div>
                <Label htmlFor="risk-per-trade">Risk Per Trade (₹)</Label>
                <Input
                  id="risk-per-trade"
                  type="number"
                  min="0"
                  value={riskPerTrade}
                  onChange={(e) => setRiskPerTrade(parseFloat(e.target.value))}
                  className="mt-1 rounded-sm"
                  data-testid="risk-per-trade-input"
                />
                <p className="text-xs text-gray-500 mt-1">0 = uses fixed qty, else auto-sizes position</p>
              </div>

              <div>
                <Label htmlFor="trail-start">Trail Start Profit</Label>
                <Input
                  id="trail-start"
                  type="number"
                  value={trailStart}
                  onChange={(e) => setTrailStart(parseFloat(e.target.value))}
                  className="mt-1 rounded-sm"
                  data-testid="trail-start-input"
                />
                <p className="text-xs text-gray-500 mt-1">Points</p>
              </div>

              <div>
                <Label htmlFor="trail-step">Trail Step</Label>
                <Input
                  id="trail-step"
                  type="number"
                  value={trailStep}
                  onChange={(e) => setTrailStep(parseFloat(e.target.value))}
                  className="mt-1 rounded-sm"
                  data-testid="trail-step-input"
                />
                <p className="text-xs text-gray-500 mt-1">Points</p>
              </div>

              <div>
                <Label htmlFor="target-points">Target Points</Label>
                <Input
                  id="target-points"
                  type="number"
                  min="0"
                  value={targetPoints}
                  onChange={(e) => setTargetPoints(parseFloat(e.target.value))}
                  className="mt-1 rounded-sm"
                  data-testid="target-points-input"
                />
                <p className="text-xs text-gray-500 mt-1">0 = disabled</p>
              </div>
            </div>

            <div className="flex justify-end pt-4 border-t border-gray-100">
              <Button
                onClick={handleSaveRiskParams}
                disabled={saving}
                size="sm"
                className="rounded-sm btn-active"
                data-testid="save-risk-params-btn"
              >
                <Save className="w-3 h-3 mr-1" />
                {saving ? "Saving..." : "Save Risk Parameters"}
              </Button>
            </div>
          </TabsContent>

          {/* Strategy Parameters Tab */}
          <TabsContent value="strategy" className="space-y-4 mt-6 bg-white p-6 rounded-lg border border-gray-200">
            <div className="space-y-3 p-4 bg-gray-50 rounded-sm border border-gray-100">
              <div className="text-sm font-medium text-gray-900">Saved Strategies</div>
              <div className="text-xs text-gray-500">
                Strategy = saved snapshot of settings. Indicator controls entries; sizing/exits are in the Risk tab.
              </div>
              <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
                <div className="space-y-1">
                  <Label htmlFor="strategy-name" className="text-xs text-gray-600">Strategy Name</Label>
                  <Input
                    id="strategy-name"
                    placeholder="e.g. ST+MACD Conservative"
                    value={strategyName}
                    onChange={(e) => setStrategyName(e.target.value)}
                    className="rounded-sm"
                    data-testid="strategy-name-input"
                  />
                  <p className="text-xs text-gray-500">Saves a snapshot (no credentials)</p>
                </div>

                <div className="space-y-1">
                  <Label className="text-xs text-gray-600">Select Strategy</Label>
                  <Select value={String(selectedStrategyId)} onValueChange={setSelectedStrategyId}>
                    <SelectTrigger className="w-full rounded-sm" data-testid="strategy-select">
                      <SelectValue placeholder={strategiesLoading ? "Loading..." : "Choose"} />
                    </SelectTrigger>
                    <SelectContent>
                      {(strategies || []).map((s) => (
                        <SelectItem key={s.id} value={String(s.id)}>
                          {s.name}
                        </SelectItem>
                      ))}
                    </SelectContent>
                  </Select>
                </div>
              </div>

              <input
                ref={fileInputRef}
                type="file"
                accept="application/json"
                className="hidden"
                onChange={handleImportFile}
              />

              <div className="flex flex-wrap gap-2">
                <Button
                  onClick={handleSaveStrategy}
                  disabled={strategiesLoading}
                  size="sm"
                  className="rounded-sm btn-active"
                  data-testid="save-strategy-btn"
                >
                  <Save className="w-3 h-3 mr-1" />
                  Save Strategy
                </Button>
                <Button
                  onClick={handleApplyOnly}
                  disabled={strategiesLoading || !selectedStrategyId || !canApplyStrategy}
                  size="sm"
                  variant="outline"
                  className="rounded-sm"
                  data-testid="apply-strategy-btn"
                >
                  Apply Only
                </Button>
                <Button
                  onClick={handleApplyAndRun}
                  disabled={strategiesLoading || !selectedStrategyId || !canApplyStrategy}
                  size="sm"
                  className="rounded-sm btn-active"
                  data-testid="apply-run-strategy-btn"
                >
                  Apply & Run
                </Button>
                <Button
                  onClick={handleRenameStrategy}
                  disabled={strategiesLoading || !selectedStrategyId}
                  size="sm"
                  variant="outline"
                  className="rounded-sm"
                  data-testid="rename-strategy-btn"
                >
                  Rename
                </Button>
                <Button
                  onClick={handleDuplicateStrategy}
                  disabled={strategiesLoading || !selectedStrategyId}
                  size="sm"
                  variant="outline"
                  className="rounded-sm"
                  data-testid="duplicate-strategy-btn"
                >
                  Duplicate
                </Button>
                <Button
                  onClick={handleDeleteStrategy}
                  disabled={strategiesLoading || !selectedStrategyId}
                  size="sm"
                  variant="destructive"
                  className="rounded-sm"
                  data-testid="delete-strategy-btn"
                >
                  Delete
                </Button>
                <Button
                  onClick={fetchStrategies}
                  disabled={strategiesLoading}
                  size="sm"
                  variant="outline"
                  className="rounded-sm"
                  data-testid="refresh-strategies-btn"
                >
                  Refresh
                </Button>
                <Button
                  onClick={handleExportStrategies}
                  disabled={strategiesLoading}
                  size="sm"
                  variant="outline"
                  className="rounded-sm"
                  data-testid="export-strategies-btn"
                >
                  Export
                </Button>
                <Button
                  onClick={handleImportClick}
                  disabled={strategiesLoading}
                  size="sm"
                  variant="outline"
                  className="rounded-sm"
                  data-testid="import-strategies-btn"
                >
                  Import
                </Button>
              </div>

              <div className="text-xs text-gray-500">
                {!canApplyStrategy
                  ? "Stop the bot and close position to apply."
                  : "Apply requires bot stopped and no open position."}
              </div>
            </div>

            <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
              <div className="space-y-2">
                <Label className="text-xs font-medium text-gray-600">Indicator Type</Label>
                <Select value={indicatorType} onValueChange={setIndicatorType}>
                  <SelectTrigger className="w-full rounded-sm" data-testid="indicator-type-select">
                    <SelectValue placeholder="Select strategy" />
                  </SelectTrigger>
                  <SelectContent>
                      <SelectItem value="score_mds">Score Engine (MDS)</SelectItem>
                  </SelectContent>
                  </Select>
                  <p className="text-xs text-gray-500">Score Engine uses internal confirmation and MDS telemetry.</p>
              </div>

              {/* SuperTrend UI removed — Score Engine is the only supported strategy */}

              {indicatorUsesMacd && (
                <>
                  <div>
                    <Label htmlFor="macd-fast">MACD Fast</Label>
                    <Input
                      id="macd-fast"
                      type="number"
                      min="1"
                      value={macdFast}
                      onChange={(e) => setMacdFast(parseInt(e.target.value) || 1)}
                      className="mt-1 rounded-sm"
                      data-testid="macd-fast-input"
                    />
                  </div>

                  <div>
                    <Label htmlFor="macd-slow">MACD Slow</Label>
                    <Input
                      id="macd-slow"
                      type="number"
                      min="1"
                      value={macdSlow}
                      onChange={(e) => setMacdSlow(parseInt(e.target.value) || 1)}
                      className="mt-1 rounded-sm"
                      data-testid="macd-slow-input"
                    />
                  </div>

                  <div>
                    <Label htmlFor="macd-signal">MACD Signal</Label>
                    <Input
                      id="macd-signal"
                      type="number"
                      min="1"
                      value={macdSignal}
                      onChange={(e) => setMacdSignal(parseInt(e.target.value) || 1)}
                      className="mt-1 rounded-sm"
                      data-testid="macd-signal-input"
                    />
                  </div>
                </>
              )}

              {indicatorUsesAdx && (
                <>
                  <div>
                    <Label htmlFor="adx-period">ADX Period</Label>
                    <Input
                      id="adx-period"
                      type="number"
                      min="1"
                      value={adxPeriod}
                      onChange={(e) => setAdxPeriod(parseInt(e.target.value) || 1)}
                      className="mt-1 rounded-sm"
                      data-testid="adx-period-input"
                    />
                  </div>

                  <div>
                    <Label htmlFor="adx-threshold">ADX Threshold</Label>
                    <Input
                      id="adx-threshold"
                      type="number"
                      min="0"
                      max="100"
                      value={adxThreshold}
                      onChange={(e) => setAdxThreshold(parseFloat(e.target.value) || 0)}
                      className="mt-1 rounded-sm"
                      data-testid="adx-threshold-input"
                    />
                    <p className="text-xs text-gray-500 mt-1">Common: 20–25 for trend strength</p>
                  </div>
                </>
              )}

              <div>
                <Label htmlFor="min-trade-gap">Min Trade Gap (seconds)</Label>
                <Input
                  id="min-trade-gap"
                  type="number"
                  min="0"
                  value={minTradeGap}
                  onChange={(e) => setMinTradeGap(parseInt(e.target.value) || 0)}
                  className="mt-1 rounded-sm"
                  data-testid="min-trade-gap-input"
                />
                <p className="text-xs text-gray-500 mt-1">0 = disabled</p>
              </div>

              {showFlipAndHtfControls && (
                <div className="flex items-center justify-between p-3 bg-gray-50 rounded-sm border border-gray-100">
                  <div>
                    <Label htmlFor="trade-only-on-flip-toggle" className="text-sm font-medium">
                      Trade Only On Flip
                    </Label>
                    <p className="text-xs text-gray-500">Entry only on SuperTrend flip</p>
                  </div>
                  <div className="flex items-center gap-2">
                    <span className="text-xs font-medium text-gray-500">Off</span>
                    <Switch
                      id="trade-only-on-flip-toggle"
                      checked={!!tradeOnlyOnFlip}
                      onCheckedChange={setTradeOnlyOnFlip}
                      data-testid="trade-only-on-flip-toggle"
                    />
                    <span className="text-xs font-medium text-emerald-700">On</span>
                  </div>
                </div>
              )}

              {showFlipAndHtfControls && (
                <div className="flex items-center justify-between p-3 bg-gray-50 rounded-sm border border-gray-100">
                  <div>
                    <Label htmlFor="htf-filter-toggle" className="text-sm font-medium">
                      HTF Filter
                    </Label>
                    <p className="text-xs text-gray-500">Require higher TF alignment</p>
                  </div>
                  <div className="flex items-center gap-2">
                    <span className="text-xs font-medium text-gray-500">Off</span>
                    <Switch
                      id="htf-filter-toggle"
                      checked={!!htfFilterEnabled}
                      onCheckedChange={setHtfFilterEnabled}
                      data-testid="htf-filter-toggle"
                    />
                    <span className="text-xs font-medium text-emerald-700">On</span>
                  </div>
                </div>
              )}

              {showFlipAndHtfControls && !!htfFilterEnabled && (
                <div>
                  <Label htmlFor="htf-filter-timeframe">HTF Timeframe (seconds)</Label>
                  <Input
                    id="htf-filter-timeframe"
                    type="number"
                    min="5"
                    value={htfFilterTimeframe}
                    onChange={(e) => setHtfFilterTimeframe(parseInt(e.target.value) || 5)}
                    className="mt-1 rounded-sm"
                    data-testid="htf-filter-timeframe-input"
                  />
                </div>
              )}

              <div>
                <Label htmlFor="min-hold-seconds">Min Hold (seconds)</Label>
                <Input
                  id="min-hold-seconds"
                  type="number"
                  min="0"
                  value={minHoldSeconds}
                  onChange={(e) => setMinHoldSeconds(parseInt(e.target.value) || 0)}
                  className="mt-1 rounded-sm"
                  data-testid="min-hold-seconds-input"
                />
                <p className="text-xs text-gray-500 mt-1">0 = disabled</p>
              </div>

              <div>
                <Label htmlFor="min-order-cooldown">Min Order Cooldown (seconds)</Label>
                <Input
                  id="min-order-cooldown"
                  type="number"
                  min="0"
                  value={minOrderCooldownSeconds}
                  onChange={(e) => setMinOrderCooldownSeconds(parseInt(e.target.value) || 0)}
                  className="mt-1 rounded-sm"
                  data-testid="min-order-cooldown-input"
                />
                <p className="text-xs text-gray-500 mt-1">0 = disabled</p>
              </div>
            </div>

            <div className="space-y-3 p-4 bg-gray-50 rounded-sm border border-gray-100">
              <div className="text-sm font-medium text-gray-900">Bypass Market Hours</div>

              <div className="flex items-center justify-between p-3 bg-white rounded-sm border border-gray-200">
                <div>
                  <Label htmlFor="bypass-market-hours-toggle" className="text-sm font-medium">
                    Run Outside Market Hours
                  </Label>
                  <p className="text-xs text-gray-500">Use with caution (paper testing)</p>
                </div>
                <div className="flex items-center gap-2">
                  <span className="text-xs font-medium text-gray-500">Off</span>
                  <Switch
                    id="bypass-market-hours-toggle"
                    checked={!!config?.bypass_market_hours}
                    onCheckedChange={handleBypassMarketHoursChange}
                    disabled={!canChangeRunContext || bypassMarketHoursUpdating}
                    data-testid="bypass-market-hours-toggle"
                  />
                  <span className="text-xs font-medium text-emerald-700">On</span>
                </div>
              </div>

              {!canChangeRunContext && (
                <p className="text-xs text-amber-600">Stop bot and close position to change</p>
              )}
            </div>

            <div className="space-y-3 p-4 bg-gray-50 rounded-sm border border-gray-100">
              <div className="text-sm font-medium text-gray-900">Paper Replay</div>

              <div className="flex items-center justify-between p-3 bg-white rounded-sm border border-gray-200">
                <div>
                  <Label htmlFor="paper-replay-enabled" className="text-sm font-medium">
                    Enable Replay (Paper Mode)
                  </Label>
                  <p className="text-xs text-gray-500">Replays historical MDS candles for a selected IST date</p>
                </div>
                <div className="flex items-center gap-2">
                  <span className="text-xs font-medium text-gray-500">Off</span>
                  <Switch
                    id="paper-replay-enabled"
                    checked={!!paperReplayEnabled}
                    onCheckedChange={setPaperReplayEnabled}
                    data-testid="paper-replay-enabled-toggle"
                  />
                  <span className="text-xs font-medium text-emerald-700">On</span>
                </div>
              </div>

              <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                <div>
                  <Label htmlFor="paper-replay-date">Replay Date (IST)</Label>
                  <Input
                    id="paper-replay-date"
                    type="date"
                    value={paperReplayDateIst}
                    onChange={(e) => setPaperReplayDateIst(e.target.value)}
                    disabled={!paperReplayEnabled}
                    className="mt-1 rounded-sm"
                    data-testid="paper-replay-date-input"
                  />
                  <p className="text-xs text-gray-500 mt-1">Used with bypass market hours</p>
                </div>
              </div>

              <div className="flex justify-end">
                <Button
                  onClick={handleSaveReplayParams}
                  disabled={saving}
                  size="sm"
                  className="rounded-sm btn-active"
                  data-testid="save-replay-params-btn"
                >
                  <Save className="w-3 h-3 mr-1" />
                  {saving ? "Saving..." : "Save Replay Settings"}
                </Button>
              </div>
            </div>

            <div className="flex justify-end pt-4 border-t border-gray-100">
              <Button
                onClick={handleSaveStrategyParams}
                disabled={saving}
                size="sm"
                className="rounded-sm btn-active"
                data-testid="save-strategy-params-btn"
              >
                <Save className="w-3 h-3 mr-1" />
                {saving ? "Saving..." : "Save Strategy Settings"}
              </Button>
            </div>
          </TabsContent>
        </Tabs>
      </div>
    </div>
  );
};

export default Settings;
