import React from 'react';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { TrendingUp, TrendingDown, Target, Zap, Anchor, AlertTriangle } from 'lucide-react';

const AnalyticsMetrics = ({ analytics }) => {
  if (!analytics) return null;

  // Safely access analytics properties with defaults
  const safeAnalytics = {
    total_pnl: analytics.total_pnl || 0,
    win_rate: analytics.win_rate || 0,
    profit_factor: analytics.profit_factor || 0,
    sharpe_ratio: analytics.sharpe_ratio || 0,
    max_drawdown: analytics.max_drawdown || 0,
    avg_trade_pnl: analytics.avg_trade_pnl || 0,
    max_consecutive_wins: analytics.max_consecutive_wins || 0,
    max_consecutive_losses: analytics.max_consecutive_losses || 0,
    avg_trades_per_day: analytics.avg_trades_per_day || 0,
    trading_days: analytics.trading_days || 0
  };

  const metrics = [
    {
      title: 'Total PnL',
      value: `₹${safeAnalytics.total_pnl}`,
      icon: <TrendingUp className="w-5 h-5" />,
      color: safeAnalytics.total_pnl >= 0 ? 'text-emerald-600' : 'text-red-600',
      bgColor: safeAnalytics.total_pnl >= 0 ? 'bg-emerald-50' : 'bg-red-50'
    },
    {
      title: 'Win Rate',
      value: `${safeAnalytics.win_rate}%`,
      icon: <Target className="w-5 h-5" />,
      color: safeAnalytics.win_rate >= 50 ? 'text-blue-600' : 'text-amber-600',
      bgColor: 'bg-blue-50'
    },
    {
      title: 'Profit Factor',
      value: (safeAnalytics.profit_factor || 0).toFixed(2),
      icon: <Zap className="w-5 h-5" />,
      color: safeAnalytics.profit_factor >= 1.5 ? 'text-emerald-600' : 'text-amber-600',
      bgColor: 'bg-emerald-50',
      subtitle: 'Total wins / Total losses'
    },
    {
      title: 'Sharpe Ratio',
      value: (safeAnalytics.sharpe_ratio || 0).toFixed(2),
      icon: <Anchor className="w-5 h-5" />,
      color: safeAnalytics.sharpe_ratio >= 1 ? 'text-purple-600' : 'text-gray-600',
      bgColor: 'bg-purple-50',
      subtitle: 'Risk-adjusted returns'
    },
    {
      title: 'Max Drawdown',
      value: `₹${Math.abs(safeAnalytics.max_drawdown)}`,
      icon: <AlertTriangle className="w-5 h-5" />,
      color: 'text-red-600',
      bgColor: 'bg-red-50',
      subtitle: 'Peak-to-trough decline'
    },
    {
      title: 'Avg Trade PnL',
      value: `₹${(safeAnalytics.avg_trade_pnl || 0).toFixed(2)}`,
      icon: <TrendingUp className="w-5 h-5" />,
      color: safeAnalytics.avg_trade_pnl >= 0 ? 'text-emerald-600' : 'text-red-600',
      bgColor: 'bg-emerald-50'
    },
    {
      title: 'Max Consecutive Wins',
      value: safeAnalytics.max_consecutive_wins,
      icon: <TrendingUp className="w-5 h-5" />,
      color: 'text-emerald-600',
      bgColor: 'bg-emerald-50'
    },
    {
      title: 'Max Consecutive Losses',
      value: safeAnalytics.max_consecutive_losses,
      icon: <TrendingDown className="w-5 h-5" />,
      color: 'text-red-600',
      bgColor: 'bg-red-50'
    },
    {
      title: 'Avg Trades/Day',
      value: (safeAnalytics.avg_trades_per_day || 0).toFixed(1),
      icon: <Target className="w-5 h-5" />,
      color: 'text-blue-600',
      bgColor: 'bg-blue-50'
    },
    {
      title: 'Trading Days',
      value: safeAnalytics.trading_days,
      icon: <Target className="w-5 h-5" />,
      color: 'text-gray-600',
      bgColor: 'bg-gray-50'
    }
  ];

  return (
    <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-5 gap-4">
      {metrics.map((metric, idx) => (
        <Card key={idx} className="border-0 shadow-sm">
          <CardHeader className={`pb-2 ${metric.bgColor}`}>
            <div className="flex items-center justify-between">
              <CardTitle className="text-xs font-medium text-gray-600">
                {metric.title}
              </CardTitle>
              <div className={`${metric.color}`}>
                {metric.icon}
              </div>
            </div>
          </CardHeader>
          <CardContent className="pt-4">
            <div className={`text-2xl font-bold ${metric.color}`}>
              {metric.value}
            </div>
            {metric.subtitle && (
              <p className="text-xs text-gray-500 mt-1">{metric.subtitle}</p>
            )}
          </CardContent>
        </Card>
      ))}
    </div>
  );
};

export default AnalyticsMetrics;
