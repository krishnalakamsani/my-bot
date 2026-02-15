import React, { useContext, useState } from "react";
import axios from "axios";
import { toast } from "sonner";
import { API, AppContext } from "@/App";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
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
import { Key, Settings, ShieldCheck, Eye, EyeOff, Save } from "lucide-react";

const SettingsPanel = ({ onClose }) => {
  const { config, updateConfig, botStatus, position } = useContext(AppContext);

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
  const [trailStart, setTrailStart] = useState(config.trail_start_profit || 0);
  const [trailStep, setTrailStep] = useState(config.trail_step || 0);
  const [targetPoints, setTargetPoints] = useState(config.target_points || 0);
  const [riskPerTrade, setRiskPerTrade] = useState(config.risk_per_trade || 0);

  const [saving, setSaving] = useState(false);
  const isFirstRender = React.useRef(true);

  // Saved strategies
  const [strategies, setStrategies] = useState([]);
  const [strategyName, setStrategyName] = useState("");
  const [selectedStrategyId, setSelectedStrategyId] = useState("");
  const [strategiesLoading, setStrategiesLoading] = useState(false);
  const fileInputRef = React.useRef(null);

  // Only sync on first mount, not on every config change to avoid overwriting user edits
  React.useEffect(() => {
    if (isFirstRender.current) {
      setOrderQty(config?.order_qty || 1);
      setMaxTrades(config?.max_trades_per_day || 5);
      setMaxLoss(config?.daily_max_loss || 2000);
      setMaxLossPerTrade(config?.max_loss_per_trade || 0);
      setInitialSL(config?.initial_stoploss || 50);
      setTrailStart(config?.trail_start_profit ?? 0);
      setTrailStep(config?.trail_step ?? 0);
      setTargetPoints(config?.target_points || 0);
      setRiskPerTrade(config?.risk_per_trade || 0);
      isFirstRender.current = false;
    }
  }, []); // Empty dependency array - only run once on mount

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

  React.useEffect(() => {
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
        // Backend filters credentials/unknown keys; we snapshot current config.
        config,
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

  // Get lot size for selected index
  const indices = [
    { name: 'NIFTY', lot_size: 50 },
    { name: 'BANKNIFTY', lot_size: 15 },
    { name: 'FINNIFTY', lot_size: 40 },
    { name: 'MIDCPNIFTY', lot_size: 75 },
    { name: 'SENSEX', lot_size: 10 }
  ];
  
  const getSelectedIndexInfo = () => {
    const selectedIndex = config?.selected_index || 'NIFTY';
    const index = indices.find(i => i.name === selectedIndex);
    return index || { lot_size: 50, strike_interval: 50 };
  };

  const indexInfo = getSelectedIndexInfo();

  return (
    <Dialog open onOpenChange={onClose}>
      <DialogContent className="sm:max-w-[550px]" data-testid="settings-modal">
        <DialogHeader>
          <DialogTitle className="flex items-center gap-2 font-[Manrope]">
            <Settings className="w-5 h-5" />
            Settings
          </DialogTitle>
          <DialogDescription>
            Configure API credentials, strategy, and risk parameters
          </DialogDescription>
        </DialogHeader>

        <Tabs defaultValue="credentials" className="w-full overflow-visible">
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
          <TabsContent value="credentials" className="space-y-4 mt-4 overflow-visible">
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
          <TabsContent value="risk" className="space-y-4 mt-4 overflow-visible">
            <div className="grid grid-cols-2 gap-4">
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
                  {orderQty} lot = {orderQty * indexInfo.lot_size} qty
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
                  value={targetPoints}
                  onChange={(e) => setTargetPoints(parseFloat(e.target.value))}
                  className="mt-1 rounded-sm"
                  data-testid="target-points-input"
                />
                <p className="text-xs text-gray-500 mt-1">0 = disabled</p>
              </div>
            </div>

            <div className="flex justify-end pt-2">
              <Button
                onClick={handleSaveRiskParams}
                disabled={saving}
                size="sm"
                className="rounded-sm btn-active"
                data-testid="save-risk-params-btn"
              >
                <Save className="w-3 h-3 mr-1" />
                {saving ? "Saving..." : "Save Parameters"}
              </Button>
            </div>
          </TabsContent>

          {/* Strategy Tab (Saved Strategies) */}
          <TabsContent value="strategy" className="space-y-4 mt-4 overflow-visible">
            <div className="space-y-3 p-4 bg-gray-50 rounded-sm border border-gray-100">
              <div className="text-sm font-medium text-gray-900">Saved Strategies</div>
              <div className="text-xs text-gray-500">
                Strategy = saved snapshot of settings. Indicator controls entries; sizing/exits are in the Risk tab.
              </div>

              <input
                ref={fileInputRef}
                type="file"
                accept="application/json"
                className="hidden"
                onChange={handleImportFile}
              />

              <div className="space-y-1">
                <Label htmlFor="strategy-name" className="text-xs text-gray-600">
                  Strategy Name
                </Label>
                <Input
                  id="strategy-name"
                  placeholder="e.g. ST+MACD Conservative"
                  value={strategyName}
                  onChange={(e) => setStrategyName(e.target.value)}
                  className="rounded-sm"
                  data-testid="strategy-name-input"
                />
                <p className="text-xs text-gray-500">Saves current settings snapshot (no credentials)</p>
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

          </TabsContent>
        </Tabs>
      </DialogContent>
    </Dialog>
  );
};

export default SettingsPanel;
