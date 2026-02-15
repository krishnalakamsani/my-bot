# Configuration and state management
import os
from pathlib import Path
try:
    from dotenv import load_dotenv
except Exception:  # pragma: no cover
    def load_dotenv(*_args, **_kwargs):
        return False

ROOT_DIR = Path(__file__).parent
load_dotenv(ROOT_DIR / '.env')

# Valid timeframe options (in seconds)
VALID_TIMEFRAMES = [5, 15, 30, 60, 300, 900]  # 5s, 15s, 30s, 1m, 5m, 15m

# Global bot state
bot_state = {
    "is_running": False,
    "mode": "paper",  # paper or live (default to paper for safety)
    "current_position": None,
    "daily_trades": 0,
    "daily_pnl": 0.0,
    "daily_max_loss_triggered": False,
    "last_supertrend_signal": None,
    "index_ltp": 0.0,
    "supertrend_value": 0.0,
    "macd_value": 0.0,  # MACD line value
    "signal_status": "waiting",  # waiting, buy (GREEN), sell (RED)
    "trailing_sl": None,
    "entry_price": 0.0,
    "current_option_ltp": 0.0,
    "max_drawdown": 0.0,
    "selected_index": "NIFTY",  # Current selected index

    # Higher timeframe (HTF) SuperTrend filter state
    "htf_supertrend_signal": None,
    "htf_supertrend_value": 0.0,
    "htf_signal_status": "waiting",

    # Score engine telemetry (MDS)
    "mds_score": 0.0,
    "mds_slope": 0.0,
    "mds_acceleration": 0.0,
    "mds_stability": 0.0,
    "mds_confidence": 0.0,
    "mds_is_choppy": False,
    "mds_direction": "NONE",

    # ADX telemetry (for ST+ADX strategy)
    "adx_value": 0.0,
}

# Configuration (can be updated from frontend)
def _env_bool(name: str, default: bool) -> bool:
    val = os.getenv(name)
    if val is None:
        return default
    return str(val).strip().lower() in ("1", "true", "yes", "y", "on")


def _env_int(name: str, default: int) -> int:
    val = os.getenv(name)
    if val is None or str(val).strip() == "":
        return default
    try:
        return int(val)
    except Exception:
        return default


def _env_float(name: str, default: float) -> float:
    val = os.getenv(name)
    if val is None or str(val).strip() == "":
        return default
    try:
        return float(val)
    except Exception:
        return default


config = {
    "dhan_access_token": "",
    "dhan_client_id": "",
    "order_qty": 1,  # Number of lots (will be multiplied by lot_size)
    "max_trades_per_day": 5,
    "daily_max_loss": 2000,
    # Stop Loss Parameters
    "initial_stoploss": 50,  # Fixed SL points below entry (0 = disabled)
    "max_loss_per_trade": 0,  # Max loss amount per trade (₹, 0 = disabled)
    "trail_start_profit": 0,  # Profit points to start trailing (0 = disabled)
    "trail_step": 0,  # Trailing step size (0 = disabled)
    # Profit Taking
    "target_points": 0,  # Target profit points (0 = disabled)

    # Market data source
    # - 'dhan' : fetch quotes directly via Dhan SDK (default, simpler setup)
    # - 'mds'  : consume candles from the separate market-data-service (TimescaleDB)
    "market_data_provider": (
        (os.getenv("MARKET_DATA_PROVIDER") or "").strip().lower()
        or "dhan"
    ),
    "mds_base_url": (os.getenv("MDS_BASE_URL", "") or "").strip(),  # e.g. http://market-data-service:8002/v1
    "mds_poll_seconds": _env_float("MDS_POLL_SECONDS", 1.0),

    # Market data persistence (backend SQLite)
    # Keep these off by default to prevent the backend DB from growing indefinitely.
    "store_tick_data": _env_bool("STORE_TICK_DATA", False),  # Persist index/option ticks to DB
    "store_candle_data": _env_bool("STORE_CANDLE_DATA", False),  # Persist derived candles/indicator telemetry to DB
    "market_data_poll_seconds": 1.0,  # Quote polling interval
    "tick_persist_interval_seconds": 1.0,  # DB write interval (throttle)
    "pause_market_data_when_closed": False,

    # DB retention (applies only to backend SQLite tables)
    "prune_db_on_startup": _env_bool("PRUNE_DB_ON_STARTUP", True),
    "max_candle_rows": _env_int("MAX_CANDLE_ROWS", 0),  # 0 = delete all if store_candle_data is False; else keep last N rows
    "max_tick_rows": _env_int("MAX_TICK_ROWS", 0),  # 0 = delete all if store_tick_data is False; else keep last N rows
    "vacuum_db_on_prune": _env_bool("VACUUM_DB_ON_PRUNE", True),

    # Internal backend tick collector (legacy). Prefer market-data-service instead.
    "enable_internal_market_data_service": _env_bool("ENABLE_INTERNAL_MARKET_DATA_SERVICE", False),

    # UI feed: keep index LTP updating even when bot is stopped (consume-only, no DB writes)
    "enable_mds_consumer": _env_bool("ENABLE_MDS_CONSUMER", False),

    # Paper replay (use recorded DB candles instead of synthetic)
    "paper_replay_enabled": False,
    "paper_replay_date_ist": "",  # YYYY-MM-DD; empty means latest candles
    "paper_replay_speed": 10.0,  # 1.0 = real-time, 10.0 = 10x faster

    # Paper mode quotes
    # If True: when market is open, paper trades will use LIVE option LTP (via Dhan)
    # instead of synthetic CE/PE premiums. This never places orders.
    # Automatically falls back to synthetic pricing if Dhan is not configured.
    "paper_use_live_option_quotes": _env_bool("PAPER_USE_LIVE_OPTION_QUOTES", True),

    # By default, do not prefetch historical candles from MDS on start — keep setup simple.
    "prefetch_candles_on_start": _env_bool("PREFETCH_CANDLES_ON_START", False),

    # Testing
    "bypass_market_hours": False,  # If True: allow running logic outside 9:15-15:30 IST
    # Signal & Indicator Settings
    "indicator_type": "score_mds",  # only score_mds is supported
    "supertrend_period": 7,
    "supertrend_multiplier": 4,
    "macd_fast": 12,
    "macd_slow": 26,
    "macd_signal": 9,
    "macd_confirmation_enabled": True,

    # ADX (used by supertrend_adx)
    "adx_period": 14,
    "adx_threshold": 25.0,

    # ScoreEngine (score_mds) bonus scoring knobs
    # These add small conviction bonuses on top of base MACD/HIST/ST scoring.
    # Set to 0 to disable.
    "mds_bonus_macd_triple": 1.0,     # MACD line + signal line + histogram all same sign
    "mds_bonus_macd_momentum": 0.5,   # MACD score + HIST score both strong and aligned
    "mds_bonus_macd_cross": 0.5,      # MACD cross event bonus
    "candle_interval": 5,  # seconds (default 5s)
    "selected_index": "NIFTY",  # Default index
    # Trade protection settings
    "min_trade_gap": 0,  # Minimum seconds between trades (0 = disabled)
    "trade_only_on_flip": True,  # Only trade on SuperTrend direction change
    "risk_per_trade": 0,  # Risk amount per trade (0 = disabled, uses fixed qty)

    # Position sizing
    # If True: lots may be reduced based on risk_per_trade and initial_stoploss.
    # If False: always use fixed lots from order_qty.
    "enable_risk_based_lots": _env_bool("ENABLE_RISK_BASED_LOTS", False),

    # Multi-timeframe (MTF) filter
    "htf_filter_enabled": True,  # If True: require HTF SuperTrend direction alignment for LTF entries
    "htf_filter_timeframe": 60,  # seconds (default 1m)

    # Toggle to run legacy (pre-tuning) thresholds for A/B replay/backtest
    # Set env `USE_LEGACY_THRESHOLDS=1` to enable legacy behavior when starting the server.
    "use_legacy_thresholds": _env_bool("USE_LEGACY_THRESHOLDS", False),

    # Exit protection
    "min_hold_seconds": 15,  # Minimum seconds to hold a new position before strategy exits (0 = disabled)

    # Order pacing
    "min_order_cooldown_seconds": 15,  # Minimum seconds between any two orders (entry/exit). Enforced for new entries.

    # Trading control
    "trading_enabled": True,  # If False: no new entries, but indicators/updates continue
}

# SQLite Database path
DB_PATH = ROOT_DIR / 'data' / 'trading.db'

# Ensure directories exist
(ROOT_DIR / 'logs').mkdir(exist_ok=True)
(ROOT_DIR / 'data').mkdir(exist_ok=True)
