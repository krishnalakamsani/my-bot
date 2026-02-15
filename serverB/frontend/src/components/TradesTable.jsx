import React, { useContext } from "react";
import { AppContext } from "@/App";
import { ScrollArea } from "@/components/ui/scroll-area";
import { TrendingUp, TrendingDown } from "lucide-react";

const TradesTable = () => {
  const { trades } = useContext(AppContext);

  const formatTime = (isoString) => {
    if (!isoString) return "—";
    const date = new Date(isoString);
    return date.toLocaleTimeString("en-IN", {
      hour: "2-digit",
      minute: "2-digit",
      second: "2-digit",
    });
  };

  const formatDate = (isoString) => {
    if (!isoString) return "";
    const date = new Date(isoString);
    return date.toLocaleDateString("en-IN", {
      day: "2-digit",
      month: "short",
    });
  };

  return (
    <div className="terminal-card flex-1" data-testid="trades-table">
      <div className="terminal-card-header">
        <h2 className="text-sm font-semibold text-gray-900 font-[Manrope]">
          Trades History
        </h2>
        <span className="text-xs text-gray-500 font-mono">
          {trades.length} trades
        </span>
      </div>

      <ScrollArea className="h-[300px]">
        <table className="terminal-table">
          <thead className="sticky top-0 z-10">
            <tr>
              <th>Time</th>
              <th>Type</th>
              <th>Strike</th>
              <th>Entry</th>
              <th>Exit</th>
              <th>P&L</th>
              <th>Reason</th>
            </tr>
          </thead>
          <tbody>
            {trades.length > 0 ? (
              trades.map((trade, index) => {
                const pnl = trade.pnl || 0;
                const isProfitable = pnl >= 0;
                const isOpen = !trade.exit_time;

                return (
                  <tr
                    key={trade.trade_id || index}
                    className={isOpen ? "bg-blue-50/50" : ""}
                    data-testid={`trade-row-${index}`}
                  >
                    <td className="font-mono text-xs">
                      <div>{formatDate(trade.entry_time)}</div>
                      <div className="text-gray-500">
                        {formatTime(trade.entry_time)}
                      </div>
                    </td>
                    <td>
                      <span
                        className={`inline-flex items-center gap-1 px-1.5 py-0.5 rounded text-xs font-medium ${
                          trade.option_type === "CE"
                            ? "bg-emerald-100 text-emerald-700"
                            : "bg-red-100 text-red-700"
                        }`}
                      >
                        {trade.option_type === "CE" ? (
                          <TrendingUp className="w-3 h-3" />
                        ) : (
                          <TrendingDown className="w-3 h-3" />
                        )}
                        {trade.option_type}
                      </span>
                    </td>
                    <td className="font-mono text-sm">{trade.strike}</td>
                    <td className="font-mono text-sm">
                      ₹{trade.entry_price?.toFixed(2)}
                    </td>
                    <td className="font-mono text-sm">
                      {trade.exit_price ? `₹${trade.exit_price.toFixed(2)}` : "—"}
                    </td>
                    <td>
                      {isOpen ? (
                        <span className="text-xs text-blue-600 font-medium">
                          OPEN
                        </span>
                      ) : (
                        <span
                          className={`font-mono text-sm font-medium ${
                            isProfitable ? "text-emerald-600" : "text-red-600"
                          }`}
                        >
                          {isProfitable ? "+" : ""}₹{pnl.toFixed(2)}
                        </span>
                      )}
                    </td>
                    <td className="text-xs text-gray-500 max-w-[100px] truncate">
                      {trade.exit_reason || "—"}
                    </td>
                  </tr>
                );
              })
            ) : (
              <tr>
                <td colSpan={7} className="text-center py-8 text-gray-400">
                  No trades yet today
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </ScrollArea>
    </div>
  );
};

export default TradesTable;
