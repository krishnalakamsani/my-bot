# Bot Service - Interface layer between API routes and TradingBot
import logging
from typing import Optional
from config import bot_state, config
from indices import get_index_config, get_available_indices
from database import save_config, load_config
import asyncio

logger = logging.getLogger(__name__)

# Lazy import to avoid circular imports
_trading_bot = None

def get_trading_bot():
    """Get or create the trading bot instance"""
    global _trading_bot
    if _trading_bot is None:
        from trading_bot import TradingBot
        _trading_bot = TradingBot()
    return _trading_bot


async def start_bot() -> dict:
    """Start the trading bot"""
    bot = get_trading_bot()
    result = await bot.start()
    logger.info(f"[BOT] Start requested: {result}")
    return result


async def stop_bot() -> dict:
    """Stop the trading bot"""
    bot = get_trading_bot()
    result = await bot.stop()
    logger.info(f"[BOT] Stop requested: {result}")
    return result


async def squareoff_position() -> dict:
    """Force square off current position"""
    bot = get_trading_bot()
    result = await bot.squareoff()
    logger.info(f"[BOT] Squareoff requested: {result}")
    return result


def get_bot_status() -> dict:
    """Get current bot status with market hour validation"""
    from utils import is_market_open, get_ist_time
    
    ist = get_ist_time()
    market_is_open = is_market_open()
    is_weekday = ist.weekday() < 5  # 0-4 = Mon-Fri, 5-6 = Sat-Sun
    
    logger.debug(f"[STATUS] Market check: Weekday={is_weekday}, Time={ist.strftime('%H:%M')}, Open={market_is_open}")
    
    return {
        "is_running": bot_state['is_running'],
        "mode": bot_state['mode'],
        "market_status": "open" if market_is_open else "closed",
        "market_details": {
            "is_weekday": is_weekday,
            "current_time_ist": ist.strftime('%H:%M:%S'),
            "trading_hours": "09:15 - 15:30 IST"
        },
        "connection_status": "connected" if config['dhan_access_token'] else "disconnected",
        "daily_max_loss_triggered": bot_state['daily_max_loss_triggered'],
        "trading_enabled": bool(config.get('trading_enabled', True)),
        "selected_index": config['selected_index'],
        "candle_interval": config['candle_interval']
    }



def get_market_data() -> dict:
    """Get current market data"""
    from datetime import datetime, timezone
    return {
        "ltp": bot_state['index_ltp'],
        # Score Engine telemetry (preferred single strategy)
        "mds_score": bot_state.get('mds_score', 0.0),
        "mds_slope": bot_state.get('mds_slope', 0.0),
        "mds_acceleration": bot_state.get('mds_acceleration', 0.0),
        "mds_stability": bot_state.get('mds_stability', 0.0),
        "mds_confidence": bot_state.get('mds_confidence', 0.0),
        "mds_is_choppy": bot_state.get('mds_is_choppy', False),
        "mds_direction": bot_state.get('mds_direction', 'NONE'),
        "selected_index": config['selected_index'],
        "timestamp": datetime.now(timezone.utc).isoformat()
    }


def get_position() -> dict:
    """Get current position info"""
    if not bot_state['current_position']:
        return {"has_position": False}
    
    index_config = get_index_config(config['selected_index'])
    qty = int(bot_state['current_position'].get('qty') or 0)
    if qty <= 0:
        qty = config['order_qty'] * index_config['lot_size']
    unrealized_pnl = (bot_state['current_option_ltp'] - bot_state['entry_price']) * qty
    
    return {
        "has_position": True,
        "option_type": bot_state['current_position'].get('option_type'),
        "strike": bot_state['current_position'].get('strike'),
        "expiry": bot_state['current_position'].get('expiry'),
        "index_name": bot_state['current_position'].get('index_name', config['selected_index']),
        "entry_price": bot_state['entry_price'],
        "current_ltp": bot_state['current_option_ltp'],
        "unrealized_pnl": unrealized_pnl,
        "trailing_sl": bot_state['trailing_sl'],
        "qty": qty
    }


def get_daily_summary() -> dict:
    """Get daily trading summary"""
    return {
        "total_trades": bot_state['daily_trades'],
        "total_pnl": bot_state['daily_pnl'],
        "max_drawdown": bot_state['max_drawdown'],
        "daily_stop_triggered": bot_state['daily_max_loss_triggered']
    }


def get_config() -> dict:
    """Get current configuration"""
    index_config = get_index_config(config['selected_index'])
    
    return {
        # API Settings
        "has_credentials": bool(config['dhan_access_token'] and config['dhan_client_id']),
        "mode": bot_state['mode'],
        # Index & Timeframe
        "selected_index": config['selected_index'],
        "candle_interval": config['candle_interval'],
        "lot_size": index_config['lot_size'],
        "strike_interval": index_config['strike_interval'],
        "expiry_type": index_config.get('expiry_type', 'weekly'),
        # Risk Parameters
        "order_qty": config['order_qty'],
        "max_trades_per_day": config['max_trades_per_day'],
        "daily_max_loss": config['daily_max_loss'],
        "max_loss_per_trade": config.get('max_loss_per_trade', 0),
        "initial_stoploss": config.get('initial_stoploss', 50),
        "trail_start_profit": config['trail_start_profit'],
        "trail_step": config['trail_step'],
        "target_points": config['target_points'],
        "risk_per_trade": config.get('risk_per_trade', 0),
        # Indicator / Strategy Settings
        "indicator_type": config.get('indicator_type', 'supertrend'),
        "supertrend_period": config.get('supertrend_period', 7),
        "supertrend_multiplier": config.get('supertrend_multiplier', 4),
        "macd_fast": int(config.get('macd_fast', 12)),
        "macd_slow": int(config.get('macd_slow', 26)),
        "macd_signal": int(config.get('macd_signal', 9)),
        "macd_confirmation_enabled": bool(config.get('macd_confirmation_enabled', True)),

        # Testing
        "bypass_market_hours": bool(config.get('bypass_market_hours', False)),

        # Market data persistence
        "store_tick_data": bool(config.get("store_tick_data", True)),
        "market_data_poll_seconds": float(config.get("market_data_poll_seconds", 1.0) or 1.0),
        "tick_persist_interval_seconds": float(config.get("tick_persist_interval_seconds", 1.0) or 1.0),
        "pause_market_data_when_closed": bool(config.get("pause_market_data_when_closed", False)),

        # Paper replay
        "paper_replay_enabled": bool(config.get("paper_replay_enabled", False)),
        "paper_replay_date_ist": str(config.get("paper_replay_date_ist", "") or ""),
        "paper_replay_speed": float(config.get("paper_replay_speed", 10.0) or 10.0),

        # MTF filter
        "htf_filter_enabled": bool(config.get('htf_filter_enabled', True)),
        "htf_filter_timeframe": int(config.get('htf_filter_timeframe', 60)),

        # Exit protection
        "min_hold_seconds": int(config.get('min_hold_seconds', 15)),

        # Order pacing
        "min_order_cooldown_seconds": int(config.get('min_order_cooldown_seconds', 15)),

        # Trading control
        "trading_enabled": bool(config.get('trading_enabled', True)),
        # ADX
        "adx_period": int(config.get('adx_period', 14) or 14),
        "adx_threshold": float(config.get('adx_threshold', 25.0) or 25.0),
    }


async def update_config_values(updates: dict) -> dict:
    """Update configuration values"""
    logger.info(f"[CONFIG] Received updates: {list(updates.keys())}")
    updated_fields = []
    
    creds_changed = False

    if updates.get('dhan_access_token') is not None:
        config['dhan_access_token'] = str(updates['dhan_access_token'] or '').strip()
        updated_fields.append('dhan_access_token')
        creds_changed = True

    if updates.get('dhan_client_id') is not None:
        config['dhan_client_id'] = str(updates['dhan_client_id'] or '').strip()
        updated_fields.append('dhan_client_id')
        creds_changed = True
        
    if updates.get('order_qty') is not None:
        qty = int(updates['order_qty'])
        # Limit to 1-10 lots for safety
        config['order_qty'] = max(1, min(10, qty))
        updated_fields.append('order_qty')
        if qty != config['order_qty']:
            logger.warning(f"[CONFIG] order_qty capped from {qty} to {config['order_qty']} (max 10 lots)")
        
    if updates.get('max_trades_per_day') is not None:
        config['max_trades_per_day'] = int(updates['max_trades_per_day'])
        updated_fields.append('max_trades_per_day')
        
    if updates.get('daily_max_loss') is not None:
        config['daily_max_loss'] = float(updates['daily_max_loss'])
        updated_fields.append('daily_max_loss')
        
    if updates.get('initial_stoploss') is not None:
        config['initial_stoploss'] = float(updates['initial_stoploss'])
        updated_fields.append('initial_stoploss')
        logger.info(f"[CONFIG] Initial stoploss changed to: {config['initial_stoploss']} pts")
        
    if updates.get('max_loss_per_trade') is not None:
        config['max_loss_per_trade'] = float(updates['max_loss_per_trade'])
        updated_fields.append('max_loss_per_trade')
        logger.info(f"[CONFIG] Max loss per trade changed to: ₹{config['max_loss_per_trade']}")
        
    if updates.get('trail_start_profit') is not None:
        config['trail_start_profit'] = float(updates['trail_start_profit'])
        updated_fields.append('trail_start_profit')
        logger.info(f"[CONFIG] Trail start profit changed to: {config['trail_start_profit']} pts")
        
    if updates.get('trail_step') is not None:
        config['trail_step'] = float(updates['trail_step'])
        updated_fields.append('trail_step')
        logger.info(f"[CONFIG] Trail step changed to: {config['trail_step']} pts")
    
    if updates.get('target_points') is not None:
        config['target_points'] = float(updates['target_points'])
        updated_fields.append('target_points')
        logger.info(f"[CONFIG] Target points changed to: {config['target_points']}")
        
    if updates.get('risk_per_trade') is not None:
        config['risk_per_trade'] = float(updates['risk_per_trade'])
        updated_fields.append('risk_per_trade')
        logger.info(f"[CONFIG] Risk per trade changed to: ₹{config['risk_per_trade']}")

    if updates.get('enable_risk_based_lots') is not None:
        config['enable_risk_based_lots'] = bool(updates['enable_risk_based_lots'])
        updated_fields.append('enable_risk_based_lots')
        logger.info(f"[CONFIG] Enable risk-based lots set to: {config['enable_risk_based_lots']}")

    if updates.get('trading_enabled') is not None:
        config['trading_enabled'] = bool(updates['trading_enabled'])
        updated_fields.append('trading_enabled')
        logger.info(f"[CONFIG] Trading enabled set to: {config['trading_enabled']}")

    if updates.get('htf_filter_enabled') is not None:
        config['htf_filter_enabled'] = bool(updates['htf_filter_enabled'])
        updated_fields.append('htf_filter_enabled')
        logger.info(f"[CONFIG] HTF filter enabled set to: {config['htf_filter_enabled']}")

    if updates.get('htf_filter_timeframe') is not None:
        tf = int(updates['htf_filter_timeframe'])
        # Current implementation supports 60s HTF cleanly; keep it constrained for safety.
        if tf != 60:
            logger.warning(f"[CONFIG] Unsupported HTF timeframe: {tf}s. Using 60s")
            tf = 60
        config['htf_filter_timeframe'] = tf
        updated_fields.append('htf_filter_timeframe')
        logger.info(f"[CONFIG] HTF filter timeframe set to: {config['htf_filter_timeframe']}s")

    if updates.get('min_hold_seconds') is not None:
        mhs = max(0, int(updates['min_hold_seconds']))
        config['min_hold_seconds'] = mhs
        updated_fields.append('min_hold_seconds')
        logger.info(f"[CONFIG] Min hold seconds set to: {config['min_hold_seconds']}s")

    if updates.get('min_order_cooldown_seconds') is not None:
        cooldown = max(0, int(updates['min_order_cooldown_seconds']))
        config['min_order_cooldown_seconds'] = cooldown
        updated_fields.append('min_order_cooldown_seconds')
        logger.info(f"[CONFIG] Min order cooldown set to: {config['min_order_cooldown_seconds']}s")

    if updates.get('bypass_market_hours') is not None:
        config['bypass_market_hours'] = str(updates['bypass_market_hours']).lower() in ('true', '1', 'yes')
        updated_fields.append('bypass_market_hours')
        logger.warning(f"[CONFIG] Bypass market hours: {config['bypass_market_hours']}")

    if updates.get("store_tick_data") is not None:
        config["store_tick_data"] = bool(updates["store_tick_data"])
        updated_fields.append("store_tick_data")
        logger.info(f"[CONFIG] Store tick data: {config['store_tick_data']}")

    if updates.get("market_data_poll_seconds") is not None:
        v = float(updates["market_data_poll_seconds"])
        config["market_data_poll_seconds"] = max(0.25, min(5.0, v))
        updated_fields.append("market_data_poll_seconds")
        logger.info(f"[CONFIG] Market data poll seconds: {config['market_data_poll_seconds']}")

    if updates.get("tick_persist_interval_seconds") is not None:
        v = float(updates["tick_persist_interval_seconds"])
        config["tick_persist_interval_seconds"] = max(0.25, min(10.0, v))
        updated_fields.append("tick_persist_interval_seconds")
        logger.info(f"[CONFIG] Tick persist interval seconds: {config['tick_persist_interval_seconds']}")

    if updates.get("pause_market_data_when_closed") is not None:
        config["pause_market_data_when_closed"] = bool(updates["pause_market_data_when_closed"])
        updated_fields.append("pause_market_data_when_closed")
        logger.info(f"[CONFIG] Pause market data when closed: {config['pause_market_data_when_closed']}")

    if updates.get("paper_replay_enabled") is not None:
        config["paper_replay_enabled"] = bool(updates["paper_replay_enabled"])
        updated_fields.append("paper_replay_enabled")
        logger.warning(f"[CONFIG] Paper replay enabled: {config['paper_replay_enabled']}")

    if updates.get("paper_replay_date_ist") is not None:
        config["paper_replay_date_ist"] = str(updates["paper_replay_date_ist"] or "")
        updated_fields.append("paper_replay_date_ist")
        logger.info(f"[CONFIG] Paper replay date (IST): {config['paper_replay_date_ist']}")

    if updates.get("paper_replay_speed") is not None:
        v = float(updates["paper_replay_speed"])
        config["paper_replay_speed"] = max(0.1, min(100.0, v))
        updated_fields.append("paper_replay_speed")
        logger.info(f"[CONFIG] Paper replay speed: {config['paper_replay_speed']}x")
        
    if updates.get('selected_index') is not None:
        new_index = updates['selected_index'].upper()
        available = get_available_indices()
        if new_index in available:
            config['selected_index'] = new_index
            bot_state['selected_index'] = new_index
            updated_fields.append('selected_index')
            logger.info(f"[CONFIG] Index changed to: {new_index}")

            # Log warmup and trigger asynchronous seeding of indicators when the bot is running
            try:
                logger.info(f"[WARMUP] Engine warming up for {new_index} (seeding indicators)")
                bot = get_trading_bot()
                if getattr(bot, 'running', False):
                    async def _seed_now():
                        try:
                            await bot._seed_indicators_from_mds_history()
                            logger.info(f"[WARMUP] Seed complete for {new_index}")
                        except Exception as e:
                            logger.exception(f"[WARMUP] Seed failed for {new_index}: {e}")
                    asyncio.create_task(_seed_now())
                else:
                    logger.info(f"[WARMUP] Bot not running; indicators will be seeded on next start for {new_index}")
            except Exception:
                logger.exception(f"[WARMUP] Failed to initiate warmup for {new_index}")
        else:
            logger.warning(f"[CONFIG] Invalid index: {new_index}. Available: {available}")
            
    if updates.get('candle_interval') is not None:
        valid_intervals = [5, 15, 30, 60, 300, 900]  # 5s, 15s, 30s, 1m, 5m, 15m
        new_interval = int(updates['candle_interval'])
        if new_interval in valid_intervals:
            config['candle_interval'] = new_interval
            updated_fields.append('candle_interval')
            logger.info(f"[CONFIG] Candle interval changed to: {new_interval}s")
            # Reset indicator when interval changes
            bot = get_trading_bot()
            bot.reset_indicator()
        else:
            logger.warning(f"[CONFIG] Invalid interval: {new_interval}. Valid: {valid_intervals}")
    
    if updates.get('indicator_type') is not None:
        new_indicator = str(updates['indicator_type']).lower()
        if new_indicator in ('score_mds',):
            config['indicator_type'] = new_indicator
            updated_fields.append('indicator_type')
            logger.info(f"[CONFIG] Indicator changed to: {new_indicator}")
            # Re-initialize indicators
            bot = get_trading_bot()
            bot._initialize_indicator()
        else:
            logger.warning(f"[CONFIG] Invalid indicator: {new_indicator}. Supported: 'score_mds'")

    if updates.get('macd_confirmation_enabled') is not None:
        config['macd_confirmation_enabled'] = str(updates['macd_confirmation_enabled']).lower() in ('true', '1', 'yes')
        updated_fields.append('macd_confirmation_enabled')
        logger.info(f"[CONFIG] MACD confirmation enabled: {config['macd_confirmation_enabled']}")
    
    # Update indicator parameters if provided
    indicator_params = {
        'supertrend_period': int,
        'supertrend_multiplier': float,
        'macd_fast': int,
        'macd_slow': int,
        'macd_signal': int,
        'adx_period': int,
        'adx_threshold': float,
    }
    
    for param, param_type in indicator_params.items():
        if updates.get(param) is not None:
            try:
                config[param] = param_type(updates[param])
                updated_fields.append(param)
                logger.info(f"[CONFIG] {param} changed to: {config[param]}")
            except (ValueError, TypeError) as e:
                logger.warning(f"[CONFIG] Invalid value for {param}: {e}")
    
    await save_config()
    logger.info(f"[CONFIG] Updated: {updated_fields}")

    # If credentials changed while bot is running, re-init Dhan immediately when appropriate.
    # For live mode we require Dhan; for paper mode, re-init when paper is configured to use
    # live option/quote data (`paper_use_live_option_quotes`). This prevents the bot from
    # continuing with a stale/None Dhan client when live quotes are desired in paper runs.
    try:
        if creds_changed and bot_state.get('is_running'):
            bot = get_trading_bot()
            if bot_state.get('mode') == 'live':
                ok = bot.initialize_dhan()
                if ok:
                    logger.info("[CONFIG] Dhan client re-initialized after credentials update (live)")
                else:
                    logger.warning("[CONFIG] Dhan client NOT initialized after credentials update (check creds)")
            elif bot_state.get('mode') == 'paper' and bool(config.get('paper_use_live_option_quotes', True)):
                # Attempt to initialize Dhan for quote-only usage in paper mode; non-fatal.
                try:
                    ok = bot.initialize_dhan()
                    if ok:
                        logger.info("[CONFIG] Dhan client initialized for paper live-quote usage")
                    else:
                        logger.warning("[CONFIG] Dhan client NOT initialized for paper live-quote usage")
                except Exception:
                    logger.warning("[CONFIG] Exception while initializing Dhan for paper live-quote usage")
    except Exception as e:
        logger.warning(f"[CONFIG] Failed to re-initialize Dhan after credentials update: {e}")

    # If user changed selected index or timeframe from frontend, and bot is not running,
    # If user changed selected index or timeframe from frontend, and bot is not running,
    # optionally start the bot automatically so that market data (Dhan/MDS) and ScoreEngine
    # begin processing immediately. Auto-start is now gated by the
    # `auto_start_on_config_change` flag (default: False) to avoid surprising behaviour
    # when users merely change UI selections.
    try:
        if (('selected_index' in updated_fields) or ('candle_interval' in updated_fields)) and not bot_state.get('is_running'):
            if not bool(config.get('auto_start_on_config_change', False)):
                logger.debug("[CONFIG] Auto-start disabled by config (auto_start_on_config_change=False)")
                # Skip auto-start unless explicitly enabled by config
                await save_config()
                return {"status": "success", "message": "Configuration updated", "updated": updated_fields}
            can_start = False
            if bot_state.get('mode') == 'paper':
                can_start = True
            elif bot_state.get('mode') == 'live':
                if str(config.get('dhan_access_token') or '').strip() and str(config.get('dhan_client_id') or '').strip():
                    can_start = True

            if can_start:
                try:
                    bot = get_trading_bot()
                    # Start asynchronously so API response returns promptly.
                    asyncio.create_task(bot.start())
                    logger.info(f"[CONFIG] Auto-starting bot after index/timeframe update: Index={config.get('selected_index')} TF={config.get('candle_interval')}")
                except Exception:
                    logger.exception("[CONFIG] Failed to auto-start bot after config change")
    except Exception:
        logger.exception("[CONFIG] Error while attempting auto-start logic")
    
    return {"status": "success", "message": "Configuration updated", "updated": updated_fields}


async def set_trading_mode(mode: str) -> dict:
    """Set trading mode (paper/live)"""
    if bot_state['current_position']:
        return {"status": "error", "message": "Cannot change mode with open position"}
    
    if mode not in ['paper', 'live']:
        return {"status": "error", "message": "Invalid mode. Use 'paper' or 'live'"}
    
    # When switching to live mode, ensure credentials are present and initialize the client
    # if the bot is already running.
    if mode == 'live':
        if not (str(config.get('dhan_access_token') or '').strip() and str(config.get('dhan_client_id') or '').strip()):
            return {"status": "error", "message": "Dhan credentials not configured. Update credentials first."}

        if bot_state.get('is_running'):
            try:
                bot = get_trading_bot()
                if not bot.initialize_dhan():
                    return {"status": "error", "message": "Failed to initialize Dhan API. Check credentials/SDK."}
            except Exception as e:
                return {"status": "error", "message": f"Failed to initialize Dhan API: {e}"}

    bot_state['mode'] = mode
    logger.info(f"[CONFIG] Trading mode changed to: {mode}")

    # Safety: when switching to paper, drop any existing Dhan client reference.
    if mode == 'paper':
        try:
            bot = get_trading_bot()
            bot.dhan = None
        except Exception:
            pass

    return {"status": "success", "mode": mode}


def get_available_indices_list() -> list:
    """Get list of available indices with their config"""
    from indices import INDICES
    
    result = []
    for name, cfg in INDICES.items():
        result.append({
            "name": name,
            "display_name": cfg['name'],
            "lot_size": cfg['lot_size'],
            "strike_interval": cfg['strike_interval'],
            "expiry_type": cfg.get('expiry_type', 'weekly'),
            "expiry_day": cfg.get('expiry_day', 1)
        })
    return result


def get_available_timeframes() -> list:
    """Get list of available timeframes"""
    return [
        {"value": 5, "label": "5 seconds"},
        {"value": 15, "label": "15 seconds"},
        {"value": 30, "label": "30 seconds"},
        {"value": 60, "label": "1 minute"},
        {"value": 300, "label": "5 minutes"},
        {"value": 900, "label": "15 minutes"}
    ]
