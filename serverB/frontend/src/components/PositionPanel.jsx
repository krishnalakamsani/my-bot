import React, { useContext } from "react";
import { AppContext } from "@/App";
import { TrendingUp, TrendingDown, Clock, Target } from "lucide-react";

const PositionPanel = () => {
  const { position, config, botStatus } = useContext(AppContext);

  if (!position?.has_position) {
    return (
      <div className="terminal-card" data-testid="position-panel">
        <div className="terminal-card-header">
          <h2 className="text-sm font-semibold text-gray-900 font-[Manrope]">
            Current Position
          </h2>
        </div>
        <div className="p-6 text-center">
          <div className="w-12 h-12 mx-auto mb-3 bg-gray-100 rounded-full flex items-center justify-center">
            <Target className="w-6 h-6 text-gray-400" />
          </div>
          <p className="text-sm text-gray-500">No open position</p>
          <p className="text-xs text-gray-400 mt-1">
            {botStatus.is_running
              ? "Waiting for signal..."
              : "Start bot to begin trading"}
          </p>
        </div>
      </div>
    );
  }

  const isProfit = position.unrealized_pnl >= 0;
  const indexName = position.index_name || config.selected_index || "NIFTY";

  return (
    <div className="terminal-card" data-testid="position-panel">
      <div className="terminal-card-header">
        <h2 className="text-sm font-semibold text-gray-900 font-[Manrope]">
          Current Position
        </h2>
        <span
          className={`status-badge ${
            position.option_type === "CE" ? "status-running" : "status-error"
          }`}
        >
          {position.option_type === "CE" ? (
            <TrendingUp className="w-3 h-3" />
          ) : (
            <TrendingDown className="w-3 h-3" />
          )}
          {indexName} {position.option_type}
        </span>
      </div>

      <div className="p-4 space-y-4">
        {/* Strike & Expiry */}
        <div className="flex justify-between items-center">
          <div>
            <p className="label-text">Strike</p>
            <p className="text-xl font-bold font-mono text-gray-900">
              {position.strike?.toLocaleString("en-IN")}
            </p>
          </div>
          <div className="text-right">
            <p className="label-text">Expiry</p>
            <p className="text-sm font-mono text-gray-700">
              {position.expiry
                ? new Date(position.expiry).toLocaleDateString("en-IN", {
                    day: "2-digit",
                    month: "short",
                  })
                : "—"}
            </p>
          </div>
        </div>

        {/* Entry & Current */}
        <div className="grid grid-cols-2 gap-3">
          <div className="bg-gray-50 p-2 rounded-sm border border-gray-100">
            <p className="label-text">Entry Price</p>
            <p className="text-base font-mono font-semibold text-gray-900">
              ₹{position.entry_price?.toFixed(2) || "—"}
            </p>
          </div>
          <div className="bg-gray-50 p-2 rounded-sm border border-gray-100">
            <p className="label-text">Current LTP</p>
            <p className="text-base font-mono font-semibold text-gray-900">
              ₹{position.current_ltp?.toFixed(2) || "—"}
            </p>
          </div>
        </div>

        {/* PnL Display */}
        <div
          className={`p-3 rounded-sm border ${
            isProfit
              ? "bg-emerald-50 border-emerald-200"
              : "bg-red-50 border-red-200"
          }`}
        >
          <div className="flex justify-between items-center">
            <p className="text-xs font-medium text-gray-600">Unrealized P&L</p>
            <p
              className={`text-lg font-bold font-mono ${
                isProfit ? "text-emerald-600" : "text-red-600"
              }`}
              data-testid="unrealized-pnl"
            >
              {isProfit ? "+" : ""}
              ₹{position.unrealized_pnl?.toFixed(2) || "0.00"}
            </p>
          </div>
        </div>

        {/* Trailing SL */}
        {position.trailing_sl && (
          <div className="flex items-center justify-between p-2 bg-amber-50 border border-amber-200 rounded-sm">
            <span className="text-xs font-medium text-amber-800">
              Trailing SL
            </span>
            <span className="text-sm font-mono font-semibold text-amber-900">
              ₹{position.trailing_sl?.toFixed(2)}
            </span>
          </div>
        )}

        {/* Quantity */}
        <div className="flex items-center justify-between text-xs text-gray-500">
          <span>Quantity: {position.qty || config.order_qty * config.lot_size}</span>
          <span className="flex items-center gap-1">
            <Clock className="w-3 h-3" />
            Active
          </span>
        </div>
      </div>
    </div>
  );
};

export default PositionPanel;
