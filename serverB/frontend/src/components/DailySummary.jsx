import React, { useContext } from "react";
import { AppContext } from "@/App";
import { TrendingUp, TrendingDown, AlertOctagon, BarChart3 } from "lucide-react";

const DailySummary = () => {
  const { summary, config } = useContext(AppContext);

  const isProfitable = summary.total_pnl >= 0;

  return (
    <div className="terminal-card" data-testid="daily-summary">
      <div className="terminal-card-header">
        <div className="flex items-center gap-2">
          <BarChart3 className="w-4 h-4 text-blue-600" />
          <h2 className="text-sm font-semibold text-gray-900 font-[Manrope]">
            Daily Summary
          </h2>
        </div>
        <span className="text-xs text-gray-500 font-mono">
          {new Date().toLocaleDateString("en-IN", {
            weekday: "short",
            day: "numeric",
            month: "short",
          })}
        </span>
      </div>

      <div className="p-4 space-y-4">
        {/* Total P&L */}
        <div className="bg-gray-50 rounded-sm p-3 border border-gray-100">
          <p className="label-text mb-1">Total P&L</p>
          <div className="flex items-center gap-2">
            {isProfitable ? (
              <TrendingUp className="w-5 h-5 text-emerald-600" />
            ) : (
              <TrendingDown className="w-5 h-5 text-red-600" />
            )}
            <p
              className={`text-2xl font-bold font-mono tracking-tight ${
                isProfitable ? "text-emerald-600" : "text-red-600"
              }`}
              data-testid="total-pnl"
            >
              {isProfitable ? "+" : ""}₹{summary.total_pnl?.toFixed(2) || "0.00"}
            </p>
          </div>
        </div>

        {/* Stats Grid */}
        <div className="grid grid-cols-2 gap-3">
          <div>
            <p className="label-text">Total Trades</p>
            <p className="value-text" data-testid="total-trades">
              {summary.total_trades || 0}
              <span className="text-xs text-gray-400 ml-1">
                / {config.max_trades_per_day}
              </span>
            </p>
          </div>

          <div>
            <p className="label-text">Max Drawdown</p>
            <p
              className="value-text text-red-600"
              data-testid="max-drawdown"
            >
              ₹{summary.max_drawdown?.toFixed(2) || "0.00"}
            </p>
          </div>
        </div>

        {/* Daily Loss Limit */}
        <div className="border-t border-gray-100 pt-3">
          <div className="flex items-center justify-between">
            <div>
              <p className="label-text">Loss Limit</p>
              <p className="text-sm font-mono">
                ₹{Math.abs(summary.total_pnl || 0).toFixed(0)} / ₹
                {config.daily_max_loss}
              </p>
            </div>
            <div
              className={`h-2 w-24 bg-gray-200 rounded-full overflow-hidden`}
            >
              <div
                className={`h-full transition-all ${
                  summary.daily_stop_triggered
                    ? "bg-red-500"
                    : summary.total_pnl < 0
                    ? "bg-amber-500"
                    : "bg-emerald-500"
                }`}
                style={{
                  width: `${Math.min(
                    100,
                    (Math.abs(summary.total_pnl || 0) / config.daily_max_loss) *
                      100
                  )}%`,
                }}
              />
            </div>
          </div>
        </div>

        {/* Warning if daily stop triggered */}
        {summary.daily_stop_triggered && (
          <div className="flex items-center gap-2 p-2 bg-red-50 border border-red-200 rounded-sm text-red-700 text-xs">
            <AlertOctagon className="w-4 h-4" />
            <span className="font-medium">
              Daily loss limit reached. Trading stopped.
            </span>
          </div>
        )}
      </div>
    </div>
  );
};

export default DailySummary;
