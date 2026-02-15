import React, { useContext, useEffect, useState, useRef } from "react";
import { AppContext } from "@/App";
import { Activity, Circle } from "lucide-react";
import {
  AreaChart,
  Area,
  XAxis,
  YAxis,
  ResponsiveContainer,
  ReferenceLine,
} from "recharts";

const NiftyTracker = () => {
  const { marketData, config, botStatus } = useContext(AppContext);
  const [priceHistory, setPriceHistory] = useState([]);
  const [flashClass, setFlashClass] = useState("");
  const prevLtpRef = useRef(marketData.ltp);

  // Format timeframe for display
  const formatTimeframe = (seconds) => {
    if (seconds < 60) return `${seconds} seconds`;
    if (seconds < 3600) return `${seconds / 60} minute${seconds > 60 ? 's' : ''}`;
    return `${seconds / 3600} hour${seconds > 3600 ? 's' : ''}`;
  };

  // Update price history for chart
  useEffect(() => {
    if (marketData.ltp > 0) {
      setPriceHistory((prev) => {
        const newEntry = {
          time: new Date().toLocaleTimeString("en-IN", {
            hour: "2-digit",
            minute: "2-digit",
            second: "2-digit",
          }),
          price: marketData.ltp,
          // keep history of MDS score for reference
          mds_score: marketData.mds_score ?? 0,
        };

        const updated = [...prev, newEntry].slice(-60); // Keep last 60 data points
        return updated;
      });

      // Flash effect on price change
      if (marketData.ltp !== prevLtpRef.current) {
        setFlashClass(
          marketData.ltp > prevLtpRef.current ? "flash-green" : "flash-red"
        );
        setTimeout(() => setFlashClass(""), 300);
        prevLtpRef.current = marketData.ltp;
      }
    }
  }, [marketData.ltp, marketData.mds_score]);

  const mdsDirection = String(botStatus?.mds_direction || "NONE");
  const isGreen = mdsDirection === 'CE';
  const signalColor = isGreen ? "#059669" : "#DC2626";
  const selectedIndex = config.selected_index || "NIFTY";
  const candleInterval = botStatus.candle_interval || config.candle_interval || 5;

  const showMds = String(config?.indicator_type || "").toLowerCase() === "score_mds";
  const mdsScore = Number(botStatus?.mds_score ?? 0);
  const mdsConfidence = Number(botStatus?.mds_confidence ?? 0);
  const mdsIsChoppy = Boolean(botStatus?.mds_is_choppy);
  const mdsColorClass =
    mdsScore > 6 ? "text-emerald-600" : mdsScore < -6 ? "text-red-600" : "text-gray-700";

  return (
    <div className="terminal-card" data-testid="nifty-tracker">
      <div className="terminal-card-header">
        <div className="flex items-center gap-2">
          <Activity className="w-4 h-4 text-blue-600" />
          <h2 className="text-sm font-semibold text-gray-900 font-[Manrope]">
            {selectedIndex} Index
          </h2>
        </div>
      </div>

      <div className="p-4">
        {/* Indicators Row */}
        <div className="flex items-center gap-6 mb-6">
          {/* LTP */}
          <div className={`rounded-sm p-3 ${flashClass}`}>
            <p className="label-text text-xs mb-1">{selectedIndex} LTP</p>
            <p
              className="text-3xl font-bold font-mono tracking-tight text-gray-900"
              data-testid="nifty-ltp"
            >
              {marketData.ltp > 0 ? marketData.ltp.toLocaleString("en-IN", {
                minimumFractionDigits: 2,
                maximumFractionDigits: 2,
              }) : "—"}
            </p>
          </div>

          {/* MDS (Score Engine) */}
          {showMds && (
            <div className="flex items-center gap-3 p-3 bg-gray-50 rounded-sm border border-gray-200">
              <div>
                <p className="label-text text-xs mb-1">MDS</p>
                <p className={`text-xl font-mono font-bold ${mdsColorClass}`} data-testid="mds-score">
                  {Number.isFinite(mdsScore) ? mdsScore.toFixed(1) : "—"}
                </p>
                <p className="text-[10px] text-gray-500 font-mono" data-testid="mds-meta">
                  Conf {Number.isFinite(mdsConfidence) ? mdsConfidence.toFixed(2) : "—"} · {mdsIsChoppy ? "CHOP" : mdsDirection}
                </p>
              </div>
            </div>
          )}

          {/* Signal (Score Engine) */}
          <div className="flex items-center gap-3 p-3 bg-gray-50 rounded-sm border border-gray-200">
            <div>
              <p className="label-text text-xs mb-1">Signal</p>
              <p className="text-xl font-mono font-bold" style={{ color: isGreen ? "#059669" : "#DC2626" }} data-testid="signal-direction">
                {mdsDirection}
              </p>
            </div>
            <Circle className="w-4 h-4 flex-shrink-0" style={{ fill: isGreen ? "#059669" : "#DC2626", color: isGreen ? "#059669" : "#DC2626" }} />
          </div>
        </div>

        {/* Price Chart */}
        <div className="h-48 bg-gray-50 border border-gray-100 rounded-sm">
          {priceHistory.length > 2 ? (
            <ResponsiveContainer width="100%" height="100%">
              <AreaChart
                data={priceHistory}
                margin={{ top: 10, right: 10, left: 0, bottom: 0 }}
              >
                <defs>
                  <linearGradient id="priceGradient" x1="0" y1="0" x2="0" y2="1">
                    <stop offset="5%" stopColor="#2563EB" stopOpacity={0.2} />
                    <stop offset="95%" stopColor="#2563EB" stopOpacity={0} />
                  </linearGradient>
                </defs>
                <XAxis
                  dataKey="time"
                  tick={{ fontSize: 10, fill: "#9CA3AF" }}
                  axisLine={false}
                  tickLine={false}
                  interval="preserveStartEnd"
                />
                <YAxis
                  domain={["auto", "auto"]}
                  tick={{ fontSize: 10, fill: "#9CA3AF" }}
                  axisLine={false}
                  tickLine={false}
                  width={60}
                  tickFormatter={(value) => value.toFixed(0)}
                />
                {/* No SuperTrend reference line when using Score Engine */}
                <Area
                  type="monotone"
                  dataKey="price"
                  stroke="#2563EB"
                  strokeWidth={2}
                  fill="url(#priceGradient)"
                  dot={false}
                  animationDuration={0}
                />
              </AreaChart>
            </ResponsiveContainer>
          ) : (
            <div className="h-full flex items-center justify-center text-gray-400 text-sm">
              Waiting for market data...
            </div>
          )}
        </div>

        {/* Info Bar */}
        <div className="mt-3 flex items-center justify-between text-xs text-gray-500 font-mono">
          <span>Timeframe: {formatTimeframe(candleInterval)}</span>
          <span>Score Engine</span>
          <span>{priceHistory.length} candles</span>
        </div>
      </div>
    </div>
  );
};

export default NiftyTracker;
