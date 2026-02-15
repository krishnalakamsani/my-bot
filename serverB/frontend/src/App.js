import React, { useState, useEffect, useRef, useCallback } from "react";
import { BrowserRouter, Routes, Route } from "react-router-dom";
import axios from "axios";
import { Toaster } from "@/components/ui/sonner";
import { toast } from "sonner";
import Dashboard from "@/pages/Dashboard";
import TradesAnalysis from "@/pages/TradesAnalysis";
import Settings from "@/pages/Settings";
import "@/App.css";

// Use environment variable or default to relative path for production
const BACKEND_URL = process.env.REACT_APP_BACKEND_URL || '';
export const API = `${BACKEND_URL}/api`;

// WebSocket URL - handle both relative and absolute URLs
const getWsUrl = () => {
  if (!BACKEND_URL || BACKEND_URL === '') {
    // Use relative WebSocket URL based on current page location
    const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
    return `${protocol}//${window.location.host}`;
  }
  return BACKEND_URL.replace('https://', 'wss://').replace('http://', 'ws://');
};
const WS_URL = getWsUrl();

// Create context for shared state
export const AppContext = React.createContext();

function App() {
  const [botStatus, setBotStatus] = useState({
    is_running: false,
    mode: "paper",
    market_status: "closed",
    connection_status: "disconnected",
    daily_max_loss_triggered: false,
    trading_enabled: true,
    selected_index: "NIFTY",
    candle_interval: 5,

    // Score engine telemetry (MDS)
    mds_score: 0,
    mds_slope: 0,
    mds_acceleration: 0,
    mds_stability: 0,
    mds_confidence: 0,
    mds_is_choppy: false,
    mds_direction: "NONE"
  });
  const [marketData, setMarketData] = useState({
    ltp: 0,
    mds_score: 0,
    mds_direction: "NONE",
    mds_confidence: 0,
    selected_index: "NIFTY"
  });
  const [position, setPosition] = useState(null);
  const [trades, setTrades] = useState([]);
  const [summary, setSummary] = useState({
    total_trades: 0,
    total_pnl: 0,
    max_drawdown: 0,
    daily_stop_triggered: false
  });
  const [logs, setLogs] = useState([]);
  const [config, setConfig] = useState({
    order_qty: 1,
    max_trades_per_day: 5,
    daily_max_loss: 2000,
    max_loss_per_trade: 0,
    initial_stoploss: 50,
    trail_start_profit: 10,
    trail_step: 5,
    target_points: 0,
    risk_per_trade: 0,

    // Strategy / indicators
    indicator_type: "score_mds",
    supertrend_period: 7,
    supertrend_multiplier: 4,
    macd_fast: 12,
    macd_slow: 26,
    macd_signal: 9,
    macd_confirmation_enabled: true,

    // Trade protections
    min_trade_gap: 0,
    trade_only_on_flip: true,

    // Multi-timeframe filter
    htf_filter_enabled: true,
    htf_filter_timeframe: 60,

    // Exit / order pacing
    min_hold_seconds: 15,
    min_order_cooldown_seconds: 15,

    has_credentials: false,
    mode: "paper",
    selected_index: "NIFTY",
    candle_interval: 5,
    lot_size: 65,
    strike_interval: 50,

    // Trading control
    trading_enabled: true,

    // Testing
    bypass_market_hours: false
  });
  const [indices, setIndices] = useState([]);
  const [timeframes, setTimeframes] = useState([]);
  const [wsConnected, setWsConnected] = useState(false);
  const wsRef = useRef(null);
  const reconnectTimeoutRef = useRef(null);
  const configRef = useRef(config);

  // Keep configRef updated
  useEffect(() => {
    configRef.current = config;
  }, [config]);

  // Fetch initial data
  const fetchData = useCallback(async () => {
    try {
      const [statusRes, marketRes, positionRes, tradesRes, summaryRes, logsRes, configRes, indicesRes, timeframesRes] = await Promise.all([
        axios.get(`${API}/status`),
        axios.get(`${API}/market/nifty`),
        axios.get(`${API}/position`),
        axios.get(`${API}/trades`),  // Fetch all trades without limit
        axios.get(`${API}/summary`),
        axios.get(`${API}/logs?limit=100`),
        axios.get(`${API}/config`),
        axios.get(`${API}/indices`),
        axios.get(`${API}/timeframes`)
      ]);

      setBotStatus(statusRes.data);
      setMarketData(marketRes.data);
      setPosition(positionRes.data);
      setTrades(tradesRes.data);
      setSummary(summaryRes.data);
      setLogs(logsRes.data);
      console.log("[App.js] Config from API:", configRes.data);
      setConfig(configRes.data);
      setIndices(indicesRes.data);
      setTimeframes(timeframesRes.data);
    } catch (error) {
      console.error("Error fetching data:", error);
    }
  }, []);

  // WebSocket connection - using useEffect to handle reconnection
  useEffect(() => {
    const connectWebSocket = () => {
      if (wsRef.current?.readyState === WebSocket.OPEN) return;

      const ws = new WebSocket(`${WS_URL}/ws`);

      ws.onopen = () => {
        setWsConnected(true);
        console.log("WebSocket connected");
      };

      ws.onmessage = (event) => {
        try {
          const data = JSON.parse(event.data);
            if (data.type === "state_update") {
            const update = data.data;
            const currentConfig = configRef.current;
            setMarketData({
              ltp: update.index_ltp,
              mds_score: update.mds_score ?? 0,
              mds_direction: update.mds_direction ?? 'NONE',
              mds_confidence: update.mds_confidence ?? 0,
              selected_index: update.selected_index
            });
            setBotStatus(prev => ({
              ...prev,
              is_running: update.is_running,
              mode: update.mode,
              trading_enabled: update.trading_enabled,
              selected_index: update.selected_index,
              candle_interval: update.candle_interval,

              mds_score: update.mds_score ?? prev.mds_score,
              mds_slope: update.mds_slope ?? prev.mds_slope,
              mds_acceleration: update.mds_acceleration ?? prev.mds_acceleration,
              mds_stability: update.mds_stability ?? prev.mds_stability,
              mds_confidence: update.mds_confidence ?? prev.mds_confidence,
              mds_is_choppy: update.mds_is_choppy ?? prev.mds_is_choppy,
              mds_direction: update.mds_direction ?? prev.mds_direction
            }));
            setSummary(prev => ({
              ...prev,
              total_trades: update.daily_trades,
              total_pnl: update.daily_pnl
            }));
            if (update.position) {
              const qty = update.position.qty ?? (currentConfig.order_qty * currentConfig.lot_size);
              setPosition({
                has_position: true,
                ...update.position,
                entry_price: update.entry_price,
                current_ltp: update.current_option_ltp,
                trailing_sl: update.trailing_sl,
                qty,
                unrealized_pnl: (update.current_option_ltp - update.entry_price) * qty
              });
            } else {
              setPosition({ has_position: false });
            }
          }
        } catch (e) {
          console.error("WebSocket message parse error:", e);
        }
      };

      ws.onclose = () => {
        setWsConnected(false);
        console.log("WebSocket disconnected");
        // Reconnect after 3 seconds
        reconnectTimeoutRef.current = setTimeout(connectWebSocket, 3000);
      };

      ws.onerror = (error) => {
        console.error("WebSocket error:", error);
      };

      wsRef.current = ws;
    };

    fetchData();
    connectWebSocket();

    // Polling fallback for non-WebSocket updates
    const pollInterval = setInterval(fetchData, 5000);

    return () => {
      clearInterval(pollInterval);
      if (reconnectTimeoutRef.current) {
        clearTimeout(reconnectTimeoutRef.current);
      }
      if (wsRef.current) {
        wsRef.current.close();
      }
    };
  }, [fetchData]);

  // Bot control functions
  const startBot = async () => {
    try {
      const res = await axios.post(`${API}/bot/start`);
      toast.success(res.data.message);
      fetchData();
    } catch (error) {
      toast.error(error.response?.data?.detail || "Failed to start bot");
    }
  };

  const stopBot = async () => {
    try {
      const res = await axios.post(`${API}/bot/stop`);
      toast.success(res.data.message);
      fetchData();
    } catch (error) {
      toast.error(error.response?.data?.detail || "Failed to stop bot");
    }
  };

  const squareOff = async () => {
    try {
      const res = await axios.post(`${API}/bot/squareoff`);
      toast.success(res.data.message);
      fetchData();
    } catch (error) {
      toast.error(error.response?.data?.detail || "Failed to square off");
    }
  };

  const updateConfig = async (newConfig) => {
    try {
      await axios.post(`${API}/config/update`, newConfig);
      toast.success("Configuration updated");
      // Wait 300ms for backend to save before fetching
      await new Promise(resolve => setTimeout(resolve, 300));
      fetchData();
    } catch (error) {
      toast.error(error.response?.data?.detail || "Failed to update config");
    }
  };

  const setMode = async (mode) => {
    try {
      await axios.post(`${API}/config/mode?mode=${mode}`);
      toast.success(`Mode changed to ${mode}`);
      fetchData();
    } catch (error) {
      toast.error(error.response?.data?.detail || "Failed to change mode");
    }
  };

  const setSelectedIndex = async (indexName) => {
    try {
      await axios.post(`${API}/config/update`, { selected_index: indexName });
      toast.success(`Index changed to ${indexName}`);
      fetchData();
    } catch (error) {
      toast.error(error.response?.data?.detail || "Failed to change index");
    }
  };

  const setTimeframe = async (interval) => {
    try {
      await axios.post(`${API}/config/update`, { candle_interval: interval });
      toast.success(`Timeframe updated`);
      fetchData();
    } catch (error) {
      toast.error(error.response?.data?.detail || "Failed to change timeframe");
    }
  };

  const refreshLogs = async () => {
    try {
      const res = await axios.get(`${API}/logs?limit=100`);
      setLogs(res.data);
    } catch (error) {
      console.error("Failed to refresh logs:", error);
    }
  };

  const contextValue = {
    botStatus,
    marketData,
    niftyData: marketData, // Backward compatibility
    position,
    trades,
    summary,
    logs,
    config,
    indices,
    timeframes,
    wsConnected,
    startBot,
    stopBot,
    squareOff,
    updateConfig,
    setMode,
    setSelectedIndex,
    setTimeframe,
    refreshLogs,
    fetchData
  };

  return (
    <AppContext.Provider value={contextValue}>
      <div className="min-h-screen bg-white">
        <Toaster position="top-right" richColors />
        <BrowserRouter>
          <Routes>
            <Route path="/" element={<Dashboard />} />
            <Route path="/settings" element={<Settings />} />
            <Route path="/analysis" element={<TradesAnalysis />} />
          </Routes>
        </BrowserRouter>
      </div>
    </AppContext.Provider>
  );
}

export default App;
