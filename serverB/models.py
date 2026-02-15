# Pydantic models for API requests/responses
from pydantic import BaseModel
from typing import Optional, Dict, Any


class StrategyCreate(BaseModel):
    name: str
    # Optional override; if omitted backend can snapshot current config.
    config: Optional[Dict[str, Any]] = None


class StrategySummary(BaseModel):
    id: int
    name: str
    created_at: str
    updated_at: str


class StrategyRename(BaseModel):
    name: str


class StrategyDuplicate(BaseModel):
    name: str


class StrategiesImport(BaseModel):
    strategies: list[dict]

class ConfigUpdate(BaseModel):
    dhan_access_token: Optional[str] = None
    dhan_client_id: Optional[str] = None
    order_qty: Optional[int] = None
    max_trades_per_day: Optional[int] = None
    daily_max_loss: Optional[float] = None
    initial_stoploss: Optional[float] = None  # Fixed SL points below entry
    max_loss_per_trade: Optional[float] = None  # Max loss per trade (₹, 0=disabled)
    trail_start_profit: Optional[float] = None
    trail_step: Optional[float] = None
    target_points: Optional[float] = None  # Target profit points for exit
    risk_per_trade: Optional[float] = None  # Risk amount per trade for position sizing
    enable_risk_based_lots: Optional[bool] = None  # If True: allow risk_per_trade to reduce lots
    selected_index: Optional[str] = None
    candle_interval: Optional[int] = None  # Timeframe in seconds
    min_trade_gap: Optional[int] = None  # Minimum seconds between trades
    trade_only_on_flip: Optional[bool] = None  # Only trade on SuperTrend flip
    trading_enabled: Optional[bool] = None  # If False: no new entries

    # Multi-timeframe (MTF) filter
    htf_filter_enabled: Optional[bool] = None
    htf_filter_timeframe: Optional[int] = None  # seconds

    # Exit protection
    min_hold_seconds: Optional[int] = None  # Minimum seconds to hold before strategy exits

    # Order pacing
    min_order_cooldown_seconds: Optional[int] = None  # Minimum seconds between any two orders

    # Testing
    bypass_market_hours: Optional[bool] = None  # If True: ignore market-hours gating (paper testing)

    # Market data persistence
    store_tick_data: Optional[bool] = None
    market_data_poll_seconds: Optional[float] = None
    tick_persist_interval_seconds: Optional[float] = None
    pause_market_data_when_closed: Optional[bool] = None

    # Paper replay
    paper_replay_enabled: Optional[bool] = None
    paper_replay_date_ist: Optional[str] = None  # YYYY-MM-DD
    paper_replay_speed: Optional[float] = None

    # Indicator / strategy
    indicator_type: Optional[str] = None  # supertrend | supertrend_macd | supertrend_adx | score_mds
    supertrend_period: Optional[int] = None
    supertrend_multiplier: Optional[float] = None
    macd_fast: Optional[int] = None
    macd_slow: Optional[int] = None
    macd_signal: Optional[int] = None
    macd_confirmation_enabled: Optional[bool] = None

    # ADX
    adx_period: Optional[int] = None
    adx_threshold: Optional[float] = None

class BotStatus(BaseModel):
    is_running: bool
    mode: str
    market_status: str
    connection_status: str
    selected_index: str
    candle_interval: int

class Position(BaseModel):
    option_type: Optional[str] = None
    strike: Optional[int] = None
    expiry: Optional[str] = None
    entry_price: float = 0.0
    current_ltp: float = 0.0
    unrealized_pnl: float = 0.0
    trailing_sl: Optional[float] = None
    qty: int = 0
    index_name: Optional[str] = None

class Trade(BaseModel):
    trade_id: str
    entry_time: str
    exit_time: Optional[str] = None
    option_type: str
    strike: int
    expiry: str
    entry_price: float
    exit_price: Optional[float] = None
    pnl: Optional[float] = None
    exit_reason: Optional[str] = None
    index_name: Optional[str] = None

class DailySummary(BaseModel):
    total_trades: int = 0
    total_pnl: float = 0.0
    max_drawdown: float = 0.0
    daily_stop_triggered: bool = False

class LogEntry(BaseModel):
    timestamp: str
    level: str
    message: str
    tag: Optional[str] = None

class IndexInfo(BaseModel):
    name: str
    display_name: str
    lot_size: int
    strike_interval: int

class TimeframeInfo(BaseModel):
    value: int
    label: str
