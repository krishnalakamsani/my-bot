import React, { useState, useEffect } from 'react';
import { Card, CardContent, CardHeader, CardTitle } from '../components/ui/card';
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '../components/ui/table';
import { Badge } from '../components/ui/badge';
import { Input } from '../components/ui/input';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '../components/ui/select';
import { Tabs, TabsContent, TabsList, TabsTrigger } from '../components/ui/tabs';
import { Button } from '../components/ui/button';
import { ArrowUpRight, ArrowDownRight, TrendingUp, TrendingDown, Calendar, RefreshCw, Filter, X } from 'lucide-react';
import AnalyticsMetrics from '../components/AnalyticsMetrics';

const API_URL = process.env.REACT_APP_BACKEND_URL || '';

const TradesAnalysis = () => {
  const [analytics, setAnalytics] = useState(null);
  const [trades, setTrades] = useState([]);
  const [filteredTrades, setFilteredTrades] = useState([]);
  const [dateRangeStats, setDateRangeStats] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [filters, setFilters] = useState({
    optionType: 'all',
    exitReason: 'all',
    searchStrike: '',
    minPnL: '',
    maxPnL: '',
    startDate: '',
    endDate: '',
    mode: 'all',
    indexName: 'all'
  });

  useEffect(() => {
    fetchAnalytics();
  }, []);

  useEffect(() => {
    applyFilters();
  }, [trades, filters]);

  const fetchAnalytics = async () => {
    try {
      setLoading(true);
      const response = await fetch(`${API_URL}/api/analytics`);
      if (!response.ok) throw new Error('Failed to fetch analytics');
      const data = await response.json();
      console.log('Analytics data received:', data);
      setAnalytics(data);
      setTrades(data.trades || []);
      setError(null);
    } catch (err) {
      console.error('Error fetching analytics:', err);
      setError(err.message);
    } finally {
      setLoading(false);
    }
  };

  const calculateDateRangeStats = (trades) => {
    if (trades.length === 0) {
      return null;
    }

    const totalPnl = trades.reduce((sum, t) => sum + (t.pnl || 0), 0);
    const winningTrades = trades.filter(t => t.pnl > 0);
    const losingTrades = trades.filter(t => t.pnl < 0);
    
    const totalProfit = winningTrades.reduce((sum, t) => sum + t.pnl, 0);
    const totalLoss = Math.abs(losingTrades.reduce((sum, t) => sum + t.pnl, 0));
    
    const avgWin = winningTrades.length > 0 ? totalProfit / winningTrades.length : 0;
    const avgLoss = losingTrades.length > 0 ? totalLoss / losingTrades.length : 0;
    const winRate = trades.length > 0 ? (winningTrades.length / trades.length * 100) : 0;
    const profitFactor = totalLoss > 0 ? totalProfit / totalLoss : (totalProfit > 0 ? totalProfit : 0);
    
    // Calculate consecutive wins/losses
    let maxConsecutiveWins = 0;
    let maxConsecutiveLosses = 0;
    let currentConsecutiveWins = 0;
    let currentConsecutiveLosses = 0;
    
    trades.forEach(trade => {
      if (trade.pnl > 0) {
        currentConsecutiveWins++;
        currentConsecutiveLosses = 0;
        maxConsecutiveWins = Math.max(maxConsecutiveWins, currentConsecutiveWins);
      } else if (trade.pnl < 0) {
        currentConsecutiveLosses++;
        currentConsecutiveWins = 0;
        maxConsecutiveLosses = Math.max(maxConsecutiveLosses, currentConsecutiveLosses);
      }
    });

    // Group by exit reason
    const byExitReason = {};
    trades.forEach(trade => {
      const reason = trade.exit_reason || 'Unknown';
      if (!byExitReason[reason]) {
        byExitReason[reason] = { count: 0, pnl: 0, wins: 0 };
      }
      byExitReason[reason].count++;
      byExitReason[reason].pnl += trade.pnl || 0;
      if (trade.pnl > 0) byExitReason[reason].wins++;
    });

    // Group by day
    const byDay = {};
    trades.forEach(trade => {
      const day = new Date(trade.entry_time).toLocaleDateString('en-IN');
      if (!byDay[day]) {
        byDay[day] = { count: 0, pnl: 0, wins: 0 };
      }
      byDay[day].count++;
      byDay[day].pnl += trade.pnl || 0;
      if (trade.pnl > 0) byDay[day].wins++;
    });

    return {
      totalTrades: trades.length,
      totalPnl,
      winningTrades: winningTrades.length,
      losingTrades: losingTrades.length,
      winRate,
      avgWin,
      avgLoss,
      profitFactor,
      maxProfit: Math.max(...trades.map(t => t.pnl || 0), 0),
      maxLoss: Math.min(...trades.map(t => t.pnl || 0), 0),
      avgTradePnl: totalPnl / trades.length,
      maxConsecutiveWins,
      maxConsecutiveLosses,
      byExitReason,
      byDay,
      totalDays: Object.keys(byDay).length,
      avgTradesPerDay: Object.keys(byDay).length > 0 ? trades.length / Object.keys(byDay).length : 0
    };
  };

  const applyFilters = () => {
    let filtered = trades;

    // Filter by date range
    if (filters.startDate) {
      const startDate = new Date(filters.startDate);
      filtered = filtered.filter(t => new Date(t.entry_time) >= startDate);
    }
    if (filters.endDate) {
      const endDate = new Date(filters.endDate);
      endDate.setHours(23, 59, 59, 999);
      filtered = filtered.filter(t => new Date(t.entry_time) <= endDate);
    }

    // Filter by option type
    if (filters.optionType !== 'all') {
      filtered = filtered.filter(t => t.option_type === filters.optionType);
    }

    // Filter by exit reason
    if (filters.exitReason !== 'all') {
      filtered = filtered.filter(t => t.exit_reason === filters.exitReason);
    }

    // Filter by mode
    if (filters.mode !== 'all') {
      filtered = filtered.filter(t => t.mode === filters.mode);
    }

    // Filter by index name
    if (filters.indexName !== 'all') {
      filtered = filtered.filter(t => t.index_name === filters.indexName);
    }

    // Filter by strike
    if (filters.searchStrike) {
      filtered = filtered.filter(t => t.strike.toString().includes(filters.searchStrike));
    }

    // Filter by PnL range
    if (filters.minPnL) {
      filtered = filtered.filter(t => t.pnl >= parseFloat(filters.minPnL));
    }
    if (filters.maxPnL) {
      filtered = filtered.filter(t => t.pnl <= parseFloat(filters.maxPnL));
    }

    setFilteredTrades(filtered);
    setDateRangeStats(calculateDateRangeStats(filtered));
  };

  const handleFilterChange = (field, value) => {
    setFilters(prev => ({ ...prev, [field]: value }));
  };

  const clearFilters = () => {
    setFilters({
      optionType: 'all',
      exitReason: 'all',
      searchStrike: '',
      minPnL: '',
      maxPnL: '',
      startDate: '',
      endDate: '',
      mode: 'all',
      indexName: 'all'
    });
  };

  const setQuickDateRange = (range) => {
    const now = new Date();
    let startDate = new Date();
    
    switch(range) {
      case 'today':
        startDate = new Date(now.setHours(0, 0, 0, 0));
        break;
      case 'yesterday':
        startDate = new Date(now.setDate(now.getDate() - 1));
        startDate.setHours(0, 0, 0, 0);
        break;
      case 'week':
        startDate = new Date(now.setDate(now.getDate() - 7));
        break;
      case 'month':
        startDate = new Date(now.setMonth(now.getMonth() - 1));
        break;
      default:
        break;
    }
    
    setFilters(prev => ({
      ...prev,
      startDate: startDate.toISOString().split('T')[0],
      endDate: new Date().toISOString().split('T')[0]
    }));
  };

  const formatDate = (isoString) => {
    if (!isoString) return '-';
    const date = new Date(isoString);
    return date.toLocaleString('en-IN', {
      year: 'numeric',
      month: 'short',
      day: 'numeric',
      hour: '2-digit',
      minute: '2-digit',
      second: '2-digit'
    });
  };

  const formatPnL = (pnl) => {
    if (pnl === null || pnl === undefined) return '-';
    return pnl > 0 ? `+₹${pnl.toFixed(2)}` : `₹${pnl.toFixed(2)}`;
  };

  const StatCard = ({ label, value, subtext, icon: Icon, isPositive }) => (
    <div className="bg-gradient-to-br from-slate-50 to-slate-100 p-4 rounded-lg border border-slate-200">
      <div className="flex items-start justify-between">
        <div>
          <p className="text-sm text-slate-600 font-medium">{label}</p>
          <p className={`text-2xl font-bold mt-2 ${isPositive ? 'text-green-600' : isPositive === false ? 'text-red-600' : 'text-slate-900'}`}>
            {value}
          </p>
          {subtext && <p className="text-xs text-slate-500 mt-1">{subtext}</p>}
        </div>
        {Icon && <Icon className={`w-5 h-5 ${isPositive ? 'text-green-500' : isPositive === false ? 'text-red-500' : 'text-slate-400'}`} />}
      </div>
    </div>
  );

  if (loading) {
    return (
      <div className="flex items-center justify-center h-screen">
        <p className="text-slate-600">Loading trade analytics...</p>
      </div>
    );
  }

  if (error) {
    return (
      <div className="flex items-center justify-center h-screen">
        <p className="text-red-600">Error: {error}</p>
      </div>
    );
  }

  if (!analytics) {
    return (
      <div className="flex items-center justify-center h-screen">
        <p className="text-slate-600">No data available</p>
      </div>
    );
  }

  const uniqueExitReasons = [...new Set(trades.map(t => t.exit_reason).filter(Boolean))];
  const uniqueIndexNames = [...new Set(trades.map(t => t.index_name).filter(Boolean))];
  const displayStats = dateRangeStats || {
    totalTrades: analytics.total_trades,
    totalPnl: analytics.total_pnl,
    winningTrades: analytics.winning_trades,
    losingTrades: analytics.losing_trades,
    winRate: analytics.win_rate,
    avgWin: analytics.avg_win,
    avgLoss: analytics.avg_loss,
    profitFactor: analytics.profit_factor,
    maxProfit: analytics.max_profit,
    maxLoss: analytics.max_loss,
    avgTradePnl: analytics.avg_trade_pnl
  };

  return (
    <div className="min-h-screen bg-gradient-to-br from-slate-900 via-slate-800 to-slate-900 p-6">
      <div className="max-w-7xl mx-auto">
        {/* Header */}
        <div className="mb-6 flex justify-between items-center">
          <div>
            <h1 className="text-4xl font-bold text-white mb-2">Trade Analysis</h1>
            <p className="text-slate-400">Comprehensive trading performance review</p>
          </div>
          <Button 
            onClick={fetchAnalytics} 
            variant="outline" 
            className="bg-slate-800 border-slate-600 text-white hover:bg-slate-700"
          >
            <RefreshCw className="w-4 h-4 mr-2" />
            Refresh
          </Button>
        </div>

        {/* Quick Date Range Filters */}
        <div className="mb-4 flex gap-2 flex-wrap">
          <Button 
            size="sm" 
            variant="outline" 
            onClick={() => setQuickDateRange('today')}
            className="bg-slate-800 border-slate-600 text-white hover:bg-slate-700"
          >
            <Calendar className="w-4 h-4 mr-1" />
            Today
          </Button>
          <Button 
            size="sm" 
            variant="outline" 
            onClick={() => setQuickDateRange('yesterday')}
            className="bg-slate-800 border-slate-600 text-white hover:bg-slate-700"
          >
            Yesterday
          </Button>
          <Button 
            size="sm" 
            variant="outline" 
            onClick={() => setQuickDateRange('week')}
            className="bg-slate-800 border-slate-600 text-white hover:bg-slate-700"
          >
            Last 7 Days
          </Button>
          <Button 
            size="sm" 
            variant="outline" 
            onClick={() => setQuickDateRange('month')}
            className="bg-slate-800 border-slate-600 text-white hover:bg-slate-700"
          >
            Last 30 Days
          </Button>
          {(filters.startDate || filters.endDate || filters.optionType !== 'all' || filters.exitReason !== 'all' || 
            filters.mode !== 'all' || filters.indexName !== 'all' || filters.searchStrike || filters.minPnL || filters.maxPnL) && (
            <Button 
              size="sm" 
              variant="outline" 
              onClick={clearFilters}
              className="bg-red-900/30 border-red-600 text-red-300 hover:bg-red-900/50"
            >
              <X className="w-4 h-4 mr-1" />
              Clear Filters
            </Button>
          )}
        </div>

        <Tabs defaultValue="overview" className="w-full">
          <TabsList className="grid w-full grid-cols-2 mb-6 bg-slate-800 border border-slate-700">
            <TabsTrigger value="overview" className="text-white">Overview</TabsTrigger>
            <TabsTrigger value="trades" className="text-white">All Trades</TabsTrigger>
          </TabsList>

          {/* Overview Tab */}
          <TabsContent value="overview" className="space-y-6">
            {/* Advanced Analytics Metrics */}
            {analytics && <AnalyticsMetrics analytics={analytics} />}

            {/* Key Statistics */}
            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
              <StatCard
                label="Total P&L"
                value={`₹${displayStats.totalPnl?.toFixed(2) || '0.00'}`}
                subtext={`${displayStats.totalTrades || 0} trades${dateRangeStats ? ' (filtered)' : ''}`}
                icon={displayStats.totalPnl >= 0 ? TrendingUp : TrendingDown}
                isPositive={displayStats.totalPnl >= 0}
              />
              <StatCard
                label="Win Rate"
                value={`${displayStats.winRate?.toFixed(2) || '0.00'}%`}
                subtext={`${displayStats.winningTrades || 0} wins, ${displayStats.losingTrades || 0} losses`}
                isPositive={displayStats.winRate >= 50}
              />
              <StatCard
                label="Profit Factor"
                value={displayStats.profitFactor?.toFixed(2) || '0.00'}
                subtext="Gross Profit / Gross Loss"
                isPositive={displayStats.profitFactor >= 1.5}
              />
              <StatCard
                label="Avg P&L Per Trade"
                value={`₹${displayStats.avgTradePnl?.toFixed(2) || '0.00'}`}
                isPositive={displayStats.avgTradePnl >= 0}
              />
            </div>

            {/* Additional Stats (Date Range Specific) */}
            {dateRangeStats && (
              <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
                <StatCard
                  label="Trading Days"
                  value={dateRangeStats.totalDays}
                  subtext={`${dateRangeStats.avgTradesPerDay.toFixed(1)} trades/day`}
                />
                <StatCard
                  label="Max Consecutive Wins"
                  value={dateRangeStats.maxConsecutiveWins}
                  subtext="Best winning streak"
                  isPositive={true}
                />
                <StatCard
                  label="Max Consecutive Losses"
                  value={dateRangeStats.maxConsecutiveLosses}
                  subtext="Worst losing streak"
                  isPositive={false}
                />
                <StatCard
                  label="Avg Win / Avg Loss"
                  value={dateRangeStats.avgLoss > 0 ? `${(dateRangeStats.avgWin / dateRangeStats.avgLoss).toFixed(2)}` : '∞'}
                  subtext="Risk-Reward Ratio"
                  isPositive={dateRangeStats.avgWin > dateRangeStats.avgLoss}
                />
              </div>
            )}

            {/* Detailed Statistics */}
            <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
              <Card className="bg-slate-800 border-slate-700">
                <CardHeader>
                  <CardTitle className="text-white text-sm">Trade Details</CardTitle>
                </CardHeader>
                <CardContent className="space-y-3">
                  <div className="flex justify-between text-sm">
                    <span className="text-slate-400">Best Trade</span>
                    <span className="text-green-400 font-semibold">+₹{displayStats.maxProfit?.toFixed(2) || '0.00'}</span>
                  </div>
                  <div className="flex justify-between text-sm">
                    <span className="text-slate-400">Worst Trade</span>
                    <span className="text-red-400 font-semibold">₹{displayStats.maxLoss?.toFixed(2) || '0.00'}</span>
                  </div>
                  <div className="flex justify-between text-sm">
                    <span className="text-slate-400">Avg Win</span>
                    <span className="text-slate-200">₹{displayStats.avgWin?.toFixed(2) || '0.00'}</span>
                  </div>
                  <div className="flex justify-between text-sm">
                    <span className="text-slate-400">Avg Loss</span>
                    <span className="text-slate-200">₹{displayStats.avgLoss?.toFixed(2) || '0.00'}</span>
                  </div>
                </CardContent>
              </Card>

              <Card className="bg-slate-800 border-slate-700">
                <CardHeader>
                  <CardTitle className="text-white text-sm">Performance</CardTitle>
                </CardHeader>
                <CardContent className="space-y-3">
                  <div className="flex justify-between text-sm">
                    <span className="text-slate-400">Winning Trades</span>
                    <span className="text-green-400 font-semibold">{displayStats.winningTrades || 0}</span>
                  </div>
                  <div className="flex justify-between text-sm">
                    <span className="text-slate-400">Losing Trades</span>
                    <span className="text-red-400 font-semibold">{displayStats.losingTrades || 0}</span>
                  </div>
                  <div className="flex justify-between text-sm">
                    <span className="text-slate-400">Total Gross Profit</span>
                    <span className="text-slate-200">₹{((displayStats.avgWin || 0) * (displayStats.winningTrades || 0)).toFixed(2)}</span>
                  </div>
                  <div className="flex justify-between text-sm">
                    <span className="text-slate-400">Total Gross Loss</span>
                    <span className="text-slate-200">₹{((displayStats.avgLoss || 0) * (displayStats.losingTrades || 0)).toFixed(2)}</span>
                  </div>
                </CardContent>
              </Card>

              <Card className="bg-slate-800 border-slate-700">
                <CardHeader>
                  <CardTitle className="text-white text-sm">By Option Type</CardTitle>
                </CardHeader>
                <CardContent className="space-y-3">
                  {Object.entries(analytics.trades_by_type).map(([optType, stats]) => (
                    <div key={optType} className="flex justify-between items-center text-sm">
                      <div>
                        <span className="text-slate-400">{optType}</span>
                        <div className="text-xs text-slate-500">{stats.count} trades, {stats.win_rate.toFixed(1)}% win</div>
                      </div>
                      <span className={`font-semibold ${stats.pnl >= 0 ? 'text-green-400' : 'text-red-400'}`}>
                        {stats.pnl >= 0 ? '+' : ''}₹{stats.pnl.toFixed(2)}
                      </span>
                    </div>
                  ))}
                </CardContent>
              </Card>
            </div>

            {/* Exit Reasons Breakdown */}
            {dateRangeStats && dateRangeStats.byExitReason && (
              <Card className="bg-slate-800 border-slate-700">
                <CardHeader>
                  <CardTitle className="text-white text-sm">Performance by Exit Reason</CardTitle>
                </CardHeader>
                <CardContent>
                  <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
                    {Object.entries(dateRangeStats.byExitReason).map(([reason, stats]) => (
                      <div key={reason} className="bg-slate-700/50 p-3 rounded-lg">
                        <div className="flex justify-between items-start mb-2">
                          <span className="text-slate-300 text-sm font-medium">{reason}</span>
                          <Badge variant={stats.pnl >= 0 ? 'default' : 'destructive'} className="text-xs">
                            {stats.pnl >= 0 ? '+' : ''}₹{stats.pnl.toFixed(2)}
                          </Badge>
                        </div>
                        <div className="text-xs text-slate-400">
                          {stats.count} trades • {((stats.wins / stats.count) * 100).toFixed(1)}% win rate
                        </div>
                      </div>
                    ))}
                  </div>
                </CardContent>
              </Card>
            )}

            {/* Daily Performance */}
            {dateRangeStats && dateRangeStats.byDay && (
              <Card className="bg-slate-800 border-slate-700">
                <CardHeader>
                  <CardTitle className="text-white text-sm">Daily Performance</CardTitle>
                </CardHeader>
                <CardContent>
                  <div className="space-y-2 max-h-96 overflow-y-auto">
                    {Object.entries(dateRangeStats.byDay)
                      .sort((a, b) => new Date(b[0]) - new Date(a[0]))
                      .map(([day, stats]) => (
                      <div key={day} className="flex justify-between items-center p-3 bg-slate-700/30 rounded-lg hover:bg-slate-700/50 transition-colors">
                        <div>
                          <span className="text-slate-300 font-medium">{day}</span>
                          <div className="text-xs text-slate-400">
                            {stats.count} trades • {((stats.wins / stats.count) * 100).toFixed(1)}% win rate
                          </div>
                        </div>
                        <span className={`font-bold ${stats.pnl >= 0 ? 'text-green-400' : 'text-red-400'}`}>
                          {stats.pnl >= 0 ? '+' : ''}₹{stats.pnl.toFixed(2)}
                        </span>
                      </div>
                    ))}
                  </div>
                </CardContent>
              </Card>
            )}
          </TabsContent>

          {/* Trades Tab */}
          <TabsContent value="trades" className="space-y-4">
            {/* Filters */}
            <Card className="bg-slate-800 border-slate-700">
              <CardHeader>
                <CardTitle className="text-white flex items-center gap-2">
                  <Filter className="w-5 h-5" />
                  Filters
                </CardTitle>
              </CardHeader>
              <CardContent>
                <div className="space-y-4">
                  {/* Date Range */}
                  <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                    <div>
                      <label className="text-sm text-slate-400 mb-2 block">Start Date</label>
                      <Input
                        type="date"
                        value={filters.startDate}
                        onChange={(e) => handleFilterChange('startDate', e.target.value)}
                        className="bg-slate-700 border-slate-600 text-white"
                      />
                    </div>
                    <div>
                      <label className="text-sm text-slate-400 mb-2 block">End Date</label>
                      <Input
                        type="date"
                        value={filters.endDate}
                        onChange={(e) => handleFilterChange('endDate', e.target.value)}
                        className="bg-slate-700 border-slate-600 text-white"
                      />
                    </div>
                  </div>

                  {/* Other Filters */}
                  <div className="grid grid-cols-1 md:grid-cols-3 lg:grid-cols-5 gap-4">
                    <div>
                      <label className="text-sm text-slate-400 mb-2 block">Index</label>
                      <Select value={filters.indexName} onValueChange={(value) => handleFilterChange('indexName', value)}>
                        <SelectTrigger className="bg-slate-700 border-slate-600 text-white">
                          <SelectValue />
                        </SelectTrigger>
                        <SelectContent className="bg-slate-700 border-slate-600">
                          <SelectItem value="all" className="text-white">All Indices</SelectItem>
                          {uniqueIndexNames.map(name => (
                            <SelectItem key={name} value={name} className="text-white">{name}</SelectItem>
                          ))}
                        </SelectContent>
                      </Select>
                    </div>

                    <div>
                      <label className="text-sm text-slate-400 mb-2 block">Option Type</label>
                      <Select value={filters.optionType} onValueChange={(value) => handleFilterChange('optionType', value)}>
                        <SelectTrigger className="bg-slate-700 border-slate-600 text-white">
                          <SelectValue />
                        </SelectTrigger>
                        <SelectContent className="bg-slate-700 border-slate-600">
                          <SelectItem value="all" className="text-white">All Types</SelectItem>
                          <SelectItem value="CE" className="text-white">CE (Call)</SelectItem>
                          <SelectItem value="PE" className="text-white">PE (Put)</SelectItem>
                        </SelectContent>
                      </Select>
                    </div>

                    <div>
                      <label className="text-sm text-slate-400 mb-2 block">Exit Reason</label>
                      <Select value={filters.exitReason} onValueChange={(value) => handleFilterChange('exitReason', value)}>
                        <SelectTrigger className="bg-slate-700 border-slate-600 text-white">
                          <SelectValue />
                        </SelectTrigger>
                        <SelectContent className="bg-slate-700 border-slate-600">
                          <SelectItem value="all" className="text-white">All Reasons</SelectItem>
                          {uniqueExitReasons.map(reason => (
                            <SelectItem key={reason} value={reason} className="text-white">{reason}</SelectItem>
                          ))}
                        </SelectContent>
                      </Select>
                    </div>

                    <div>
                      <label className="text-sm text-slate-400 mb-2 block">Mode</label>
                      <Select value={filters.mode} onValueChange={(value) => handleFilterChange('mode', value)}>
                        <SelectTrigger className="bg-slate-700 border-slate-600 text-white">
                          <SelectValue />
                        </SelectTrigger>
                        <SelectContent className="bg-slate-700 border-slate-600">
                          <SelectItem value="all" className="text-white">All Modes</SelectItem>
                          <SelectItem value="paper" className="text-white">Paper</SelectItem>
                          <SelectItem value="live" className="text-white">Live</SelectItem>
                        </SelectContent>
                      </Select>
                    </div>

                    <div>
                      <label className="text-sm text-slate-400 mb-2 block">Strike</label>
                      <Input
                        type="text"
                        placeholder="Search strike..."
                        value={filters.searchStrike}
                        onChange={(e) => handleFilterChange('searchStrike', e.target.value)}
                        className="bg-slate-700 border-slate-600 text-white"
                      />
                    </div>
                  </div>

                  {/* PnL Range */}
                  <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                    <div>
                      <label className="text-sm text-slate-400 mb-2 block">Min P&L (₹)</label>
                      <Input
                        type="number"
                        placeholder="Min..."
                        value={filters.minPnL}
                        onChange={(e) => handleFilterChange('minPnL', e.target.value)}
                        className="bg-slate-700 border-slate-600 text-white"
                      />
                    </div>

                    <div>
                      <label className="text-sm text-slate-400 mb-2 block">Max P&L (₹)</label>
                      <Input
                        type="number"
                        placeholder="Max..."
                        value={filters.maxPnL}
                        onChange={(e) => handleFilterChange('maxPnL', e.target.value)}
                        className="bg-slate-700 border-slate-600 text-white"
                      />
                    </div>
                  </div>
                </div>
              </CardContent>
            </Card>

            {/* Trades Table */}
            <Card className="bg-slate-800 border-slate-700">
              <CardHeader>
                <CardTitle className="text-white">
                  All Trades <span className="text-sm text-slate-400 font-normal ml-2">({filteredTrades.length} of {trades.length})</span>
                </CardTitle>
              </CardHeader>
              <CardContent>
                <div className="overflow-x-auto">
                  <Table>
                    <TableHeader>
                      <TableRow className="border-slate-700 hover:bg-slate-700/50">
                        <TableHead className="text-slate-300 font-semibold">Entry Time</TableHead>
                        <TableHead className="text-slate-300 font-semibold">Exit Time</TableHead>
                        <TableHead className="text-slate-300 font-semibold">Index</TableHead>
                        <TableHead className="text-slate-300 font-semibold">Type</TableHead>
                        <TableHead className="text-slate-300 font-semibold">Strike</TableHead>
                        <TableHead className="text-slate-300 font-semibold">Entry</TableHead>
                        <TableHead className="text-slate-300 font-semibold">Exit</TableHead>
                        <TableHead className="text-slate-300 font-semibold">Qty</TableHead>
                        <TableHead className="text-slate-300 font-semibold">P&L</TableHead>
                        <TableHead className="text-slate-300 font-semibold">Reason</TableHead>
                        <TableHead className="text-slate-300 font-semibold">Mode</TableHead>
                      </TableRow>
                    </TableHeader>
                    <TableBody>
                      {filteredTrades.length > 0 ? (
                        filteredTrades.map((trade, idx) => (
                          <TableRow key={idx} className="border-slate-700 hover:bg-slate-700/50">
                            <TableCell className="text-slate-300 text-xs">{formatDate(trade.entry_time)}</TableCell>
                            <TableCell className="text-slate-300 text-xs">{formatDate(trade.exit_time)}</TableCell>
                            <TableCell className="text-slate-300">
                              <Badge variant="outline" className="text-xs">{trade.index_name || 'N/A'}</Badge>
                            </TableCell>
                            <TableCell>
                              <Badge variant={trade.option_type === 'CE' ? 'default' : 'secondary'}>
                                {trade.option_type}
                              </Badge>
                            </TableCell>
                            <TableCell className="text-slate-300 font-mono">{trade.strike}</TableCell>
                            <TableCell className="text-slate-300">₹{trade.entry_price.toFixed(2)}</TableCell>
                            <TableCell className="text-slate-300">₹{(trade.exit_price || 0).toFixed(2)}</TableCell>
                            <TableCell className="text-slate-300">{trade.qty}</TableCell>
                            <TableCell>
                              <span className={`font-semibold flex items-center gap-1 ${trade.pnl > 0 ? 'text-green-400' : trade.pnl < 0 ? 'text-red-400' : 'text-slate-300'}`}>
                                {trade.pnl > 0 ? <ArrowUpRight className="w-4 h-4" /> : trade.pnl < 0 ? <ArrowDownRight className="w-4 h-4" /> : null}
                                {formatPnL(trade.pnl)}
                              </span>
                            </TableCell>
                            <TableCell className="text-slate-400 text-xs">{trade.exit_reason || '-'}</TableCell>
                            <TableCell>
                              <Badge variant={trade.mode === 'live' ? 'destructive' : 'outline'} className="text-xs">
                                {trade.mode || 'paper'}
                              </Badge>
                            </TableCell>
                          </TableRow>
                        ))
                      ) : (
                        <TableRow>
                          <TableCell colSpan="11" className="text-center text-slate-400 py-8">
                            No trades match the selected filters
                          </TableCell>
                        </TableRow>
                      )}
                    </TableBody>
                  </Table>
                </div>
              </CardContent>
            </Card>
          </TabsContent>
        </Tabs>
      </div>
    </div>
  );
};

export default TradesAnalysis;
