"""Trading Bot Engine
Handles all trading logic, signal processing, and order execution.
Uses structured logging with tags for easy troubleshooting.
"""
import asyncio
from datetime import datetime, timezone, timedelta
import logging
import random

from config import bot_state, config, DB_PATH
from indices import get_index_config, round_to_strike
from utils import get_ist_time, is_market_open, can_take_new_trade, should_force_squareoff, format_timeframe
from indicators import SuperTrend, MACD, ADX
from score_engine import ScoreEngine, Candle
from strategies.runner import ScoreMdsRunner
from strategies.runtime import ClosedCandleContext, ScoreMdsRuntime, build_strategy_runtime
from dhan_api import DhanAPI
from database import save_trade, update_trade_exit

logger = logging.getLogger(__name__)

from enum import Enum
try:
    from event_bus import subscribe
except Exception:
    subscribe = lambda *a, **k: None


class State(Enum):
    IDLE = "IDLE"
    SIGNAL_GENERATED = "SIGNAL_GENERATED"
    ORDER_PLACED = "ORDER_PLACED"
    POSITION_OPEN = "POSITION_OPEN"
    EXIT_PENDING = "EXIT_PENDING"
    CLOSED = "CLOSED"


def _default_signal_handler(payload):
    # placeholder used if instance method can't be bound
    return None


class TradeContext:
    """Represents the lifecycle and metadata for a single trade attempt."""

    def __init__(self, pos_id: str, symbol: str, side: str, size: int, entry_score: float = None):
        self.pos_id = pos_id
        self.symbol = symbol
        self.side = side
        self.size = int(size or 0)
        self.entry_score = float(entry_score) if entry_score is not None else None
        self.state = State.SIGNAL_GENERATED
        self.entry_price = None
        self.entry_time = None
        self.db_id = None
        self.trailing_stop = None
        self.pnl = 0.0

    def to_dict(self):
        return {
            'pos_id': self.pos_id,
            'symbol': self.symbol,
            'side': self.side,
            'size': self.size,
            'entry_score': self.entry_score,
            'state': self.state.value if self.state else None,
            'entry_price': self.entry_price,
            'entry_time': self.entry_time.isoformat() if self.entry_time else None,
            'db_id': self.db_id,
            'pnl': self.pnl,
        }



class TradingBot:
    """Main trading bot engine"""
    
    def __init__(self):
        self.running = False
        self.task = None
        self.dhan = None
        self.current_position = None
        self.entry_price = 0.0
        self.trailing_sl = None
        self.highest_profit = 0.0
        self.indicator = None  # Will hold selected indicator
        self.htf_indicator = None  # Higher-timeframe SuperTrend (e.g., 1m filter)
        self.macd = None  # LTF MACD for confirmation
        self.adx = None  # ADX strength filter (optional)
        self.score_engine = None  # Multi-timeframe score engine (optional)
        self._st_runner = None
        self._mds_runner = None
        self._strategy_runtime = None
        self.last_exit_candle_time = None
        self.last_trade_time = None  # For min_trade_gap protection
        self.last_signal = None  # For trade_only_on_flip protection
        self._last_entries_paused_log_time = None
        self.entry_time_utc = None  # datetime for min-hold exit protection
        self.last_order_time_utc = None  # datetime for order cooldown (entry/exit pacing)
        self._paper_replay_candles = []
        self._paper_replay_pos = 0
        self._paper_replay_htf_elapsed = 0
        self._last_mds_candle_ts = None
        self._mds_htf_count = 0
        self._mds_htf_high = 0.0
        self._mds_htf_low = float('inf')
        self._mds_htf_close = 0.0
        self._initialize_indicator()
        # state machine
        self.state = State.IDLE
        self.pending_order = None
        # active TradeContext (one at a time for this simple runner)
        self.trade_context = None
        # bind instance handlers for events
        try:
            subscribe("ENTRY_SIGNAL", lambda payload: asyncio.get_event_loop().call_soon_threadsafe(self._on_entry_signal_event, payload))
            subscribe("EXIT_SIGNAL", lambda payload: asyncio.get_event_loop().call_soon_threadsafe(self._on_exit_signal_event, payload))
            subscribe("ORDER_PLACED", lambda payload: asyncio.get_event_loop().call_soon_threadsafe(self._on_order_placed_event, payload))
            subscribe("ORDER_FILLED", lambda payload: asyncio.get_event_loop().call_soon_threadsafe(self._on_order_filled_event, payload))
            subscribe("ORDER_TIMEOUT", lambda payload: asyncio.get_event_loop().call_soon_threadsafe(self._on_order_timeout_event, payload))
        except Exception:
            # non-async contexts will ignore subscriptions
            pass


    def _prefetch_candles_needed(self) -> int:
        st_period = int(config.get('supertrend_period', 7) or 7)
        macd_slow = int(config.get('macd_slow', 26) or 26)
        macd_signal = int(config.get('macd_signal', 9) or 9)

        # SuperTrend needs at least `period` candles; MACD needs slow EMA + signal EMA.
        base_needed = max(st_period + 1, macd_slow + macd_signal)

        # If HTF filter is enabled (fixed to 60s in current code), seed enough candles
        # so 1m SuperTrend is also ready.
        interval = int(config.get('candle_interval', 5) or 5)
        if bool(config.get('htf_filter_enabled', True)) and interval < 60 and (60 % max(1, interval) == 0):
            multiple_1m = 60 // max(1, interval)
            base_needed = max(base_needed, multiple_1m * (st_period + 1))

        # If score engine is selected, ensure both base TF and next TF are ready.
        if str(config.get('indicator_type', 'supertrend') or 'supertrend').strip().lower() == 'score_mds':
            try:
                base_tf = int(config.get('candle_interval', 5) or 5)
                if base_tf in ScoreEngine._TF_CHAIN:
                    chain = list(ScoreEngine._TF_CHAIN)
                    next_tf = chain[chain.index(base_tf) + 1]
                    multiple = int(next_tf) // max(1, base_tf)
                    next_tf_needed = max(st_period + 1, macd_slow + macd_signal)
                    base_needed = max(base_needed, multiple * next_tf_needed)
            except Exception:
                pass

        # Small safety cushion for flip/slope computations.
        return int(max(50, base_needed + 5))

    async def _seed_indicators_from_mds_history(self) -> None:
        if str(config.get('market_data_provider', 'dhan') or 'dhan').strip().lower() != 'mds':
            return
        if not bool(config.get('prefetch_candles_on_start', True)):
            return

        base_url = str(config.get('mds_base_url', '') or '').strip()
        if not base_url:
            return

        index_name = str(config.get('selected_index', 'NIFTY') or 'NIFTY').strip().upper()
        interval = int(config.get('candle_interval', 5) or 5)
        limit = self._prefetch_candles_needed()

        try:
            from mds_client import fetch_last_candles

            candles = await fetch_last_candles(
                base_url=base_url,
                symbol=index_name,
                timeframe_seconds=interval,
                limit=limit,
            )
        except Exception as e:
            logger.warning(f"[WARMUP] Prefetch failed (MDS): {e}")
            return

        if not candles:
            logger.info("[WARMUP] No candles returned from MDS (skipping seed)")
            return

        # Reset any MDS-derived HTF aggregation state
        self._mds_htf_count = 0
        self._mds_htf_high = 0.0
        self._mds_htf_low = float('inf')
        self._mds_htf_close = 0.0

        multiple_1m = None
        if bool(config.get('htf_filter_enabled', True)) and interval < 60 and (60 % max(1, interval) == 0):
            multiple_1m = 60 // max(1, interval)

        last_indicator_value = None
        last_signal = None
        last_mds = None

        for row in candles:
            if not isinstance(row, dict):
                continue
            try:
                high = float(row.get('high') or 0.0)
                low = float(row.get('low') or float('inf'))
                close = float(row.get('close') or 0.0)
            except Exception:
                continue

            if close <= 0 or high <= 0 or low == float('inf'):
                continue

            last_indicator_value, last_signal = self.indicator.add_candle(high, low, close)
            if self.macd:
                self.macd.add_candle(high, low, close)
            if self.adx:
                adx_val, _adx_sig = self.adx.add_candle(high, low, close)
                if adx_val is not None:
                    bot_state['adx_value'] = float(adx_val)

            if str(config.get('indicator_type') or '').strip().lower() == 'score_mds' and self.score_engine:
                try:
                    last_mds = self.score_engine.on_base_candle(Candle(high=high, low=low, close=close))
                except Exception:
                    last_mds = None

            if multiple_1m:
                self._mds_htf_count += 1
                self._mds_htf_high = max(self._mds_htf_high, high)
                self._mds_htf_low = min(self._mds_htf_low, low)
                self._mds_htf_close = close

                if self._mds_htf_count >= multiple_1m:
                    htf_value, htf_signal = self.htf_indicator.add_candle(
                        self._mds_htf_high, self._mds_htf_low, self._mds_htf_close
                    )
                    if htf_value:
                        bot_state['htf_supertrend_value'] = htf_value if isinstance(htf_value, (int, float)) else str(htf_value)
                        if htf_signal == 'GREEN':
                            bot_state['htf_signal_status'] = 'buy'
                        elif htf_signal == 'RED':
                            bot_state['htf_signal_status'] = 'sell'
                        else:
                            bot_state['htf_signal_status'] = 'waiting'
                        if htf_signal:
                            bot_state['htf_supertrend_signal'] = htf_signal

                    self._mds_htf_count = 0
                    self._mds_htf_high = 0.0
                    self._mds_htf_low = float('inf')
                    self._mds_htf_close = 0.0

        # Publish last computed values to state so UI doesn't show "waiting" on startup.
        if last_indicator_value:
            bot_state['supertrend_value'] = last_indicator_value if isinstance(last_indicator_value, (int, float)) else str(last_indicator_value)
            if last_signal == 'GREEN':
                bot_state['signal_status'] = 'buy'
            elif last_signal == 'RED':
                bot_state['signal_status'] = 'sell'
            else:
                bot_state['signal_status'] = 'waiting'
            if last_signal:
                bot_state['last_supertrend_signal'] = last_signal

        if self.macd and self.macd.last_macd is not None:
            bot_state['macd_value'] = float(self.macd.last_macd)

        if self.adx and getattr(self.adx, 'adx_values', None):
            try:
                bot_state['adx_value'] = float(self.adx.adx_values[-1])
            except Exception:
                pass

        if last_mds is not None:
            bot_state['mds_score'] = float(last_mds.score)
            bot_state['mds_slope'] = float(last_mds.slope)
            bot_state['mds_acceleration'] = float(last_mds.acceleration)
            bot_state['mds_stability'] = float(last_mds.stability)
            bot_state['mds_confidence'] = float(last_mds.confidence)
            bot_state['mds_is_choppy'] = bool(last_mds.is_choppy)
            bot_state['mds_direction'] = str(last_mds.direction)

        # Prevent immediate re-processing of the last candle on first poll.
        try:
            self._last_mds_candle_ts = str((candles[-1] or {}).get('ts') or '') or None
        except Exception:
            self._last_mds_candle_ts = None

        logger.info(f"[WARMUP] Seeded indicators from MDS history | Candles={len(candles)} Interval={interval}s")

    async def _handle_closed_candle(
        self,
        *,
        index_name: str,
        candle_number: int,
        candle_interval: int,
        high: float,
        low: float,
        close: float,
        current_candle_time: datetime,
    ) -> None:
        if not (high > 0 and low < float('inf') and close > 0):
            return

        indicator_value, signal = self.indicator.add_candle(high, low, close)
        macd_value = 0.0
        if self.macd:
            macd_line, _macd_cross = self.macd.add_candle(high, low, close)
            if macd_line is not None:
                macd_value = float(macd_line)

        adx_value = None
        if self.adx:
            try:
                adx_val, _adx_sig = self.adx.add_candle(high, low, close)
                if adx_val is not None:
                    adx_value = float(adx_val)
            except Exception:
                adx_value = None

        mds_snapshot = None
        if config.get('indicator_type') == 'score_mds' and self.score_engine:
            try:
                mds_snapshot = self.score_engine.on_base_candle(Candle(high=float(high), low=float(low), close=float(close)))

                bot_state['mds_score'] = float(mds_snapshot.score)
                bot_state['mds_slope'] = float(mds_snapshot.slope)
                bot_state['mds_acceleration'] = float(mds_snapshot.acceleration)
                bot_state['mds_stability'] = float(mds_snapshot.stability)
                bot_state['mds_confidence'] = float(mds_snapshot.confidence)
                bot_state['mds_is_choppy'] = bool(mds_snapshot.is_choppy)
                bot_state['mds_direction'] = str(mds_snapshot.direction)
            except Exception as e:
                logger.error(f"[MDS] ScoreEngine update failed: {e}", exc_info=True)
                mds_snapshot = None

        # Update state if indicator is ready
        if indicator_value:
            bot_state['supertrend_value'] = indicator_value if isinstance(indicator_value, (int, float)) else str(indicator_value)
        if self.macd and self.macd.last_macd is not None:
            bot_state['macd_value'] = float(self.macd.last_macd)
        else:
            bot_state['macd_value'] = macd_value

        if adx_value is not None:
            bot_state['adx_value'] = float(adx_value)

        # Update signal status (GREEN="buy", RED="sell", None="waiting")
        if signal == "GREEN":
            bot_state['signal_status'] = "buy"
        elif signal == "RED":
            bot_state['signal_status'] = "sell"
        else:
            bot_state['signal_status'] = "waiting"

        # Always log candle close (even while indicators warm up)
        st_txt = f"{indicator_value:.2f}" if isinstance(indicator_value, (int, float)) else "NA"
        macd_txt = f"{macd_value:.4f}" if isinstance(macd_value, (int, float)) else "NA"
        signal_txt = signal if signal else "NONE"
        logger.info(
            f"[CANDLE CLOSE #{candle_number}] {index_name} | "
            f"H={high:.2f} L={low:.2f} C={close:.2f} | "
            f"ST={st_txt} | MACD={macd_txt} | Signal={signal_txt}"
        )

        # Save candle data for analysis (optional; disabled by default to keep DB small)
        if indicator_value and config.get('store_candle_data', False):
            from database import save_candle_data

            await save_candle_data(
                candle_number=candle_number,
                index_name=index_name,
                high=high,
                low=low,
                close=close,
                supertrend_value=indicator_value,
                macd_value=macd_value,
                signal_status=bot_state['signal_status'],
                interval_seconds=int(config.get('candle_interval', candle_interval) or candle_interval),
            )

        # Candle-close trailing SL/target check regardless of signal readiness
        if self.current_position:
            option_ltp = bot_state['current_option_ltp']
            sl_hit = await self.check_trailing_sl_on_close(option_ltp)
            if sl_hit:
                self.last_exit_candle_time = current_candle_time

        runtime = self._get_strategy_runtime()
        await runtime.on_closed_candle(
            self,
            ClosedCandleContext(
                candle_interval_seconds=int(candle_interval or 0),
                current_candle_time=current_candle_time,
                close=float(close),
                signal=str(signal or '') if signal else None,
                mds_snapshot=mds_snapshot,
            ),
        )

    def _can_place_new_entry_order(self) -> bool:
        cooldown = int(config.get('min_order_cooldown_seconds', 0) or 0)
        if cooldown <= 0 or self.last_order_time_utc is None:
            return True
        elapsed = (datetime.now(timezone.utc) - self.last_order_time_utc).total_seconds()
        return elapsed >= cooldown

    def _remaining_entry_cooldown(self) -> float:
        cooldown = int(config.get('min_order_cooldown_seconds', 0) or 0)
        if cooldown <= 0 or self.last_order_time_utc is None:
            return 0.0
        elapsed = (datetime.now(timezone.utc) - self.last_order_time_utc).total_seconds()
        return max(0.0, cooldown - elapsed)

    def _min_hold_active(self) -> bool:
        min_hold = int(config.get('min_hold_seconds', 0) or 0)
        if min_hold <= 0 or self.entry_time_utc is None or not self.current_position:
            return False
        held = (datetime.now(timezone.utc) - self.entry_time_utc).total_seconds()
        return held < min_hold
    
    def initialize_dhan(self):
        """Initialize Dhan API connection"""
        if config['dhan_access_token'] and config['dhan_client_id']:
            try:
                self.dhan = DhanAPI(config['dhan_access_token'], config['dhan_client_id'])
                logger.info("[MARKET] Dhan API initialized")
                return True
            except Exception as e:
                self.dhan = None
                logger.warning(f"[ERROR] Dhan API init failed: {e}")
                return False
        logger.warning("[ERROR] Dhan API credentials not configured")
        return False

    def _paper_should_use_live_option_quotes(self) -> bool:
        if bot_state.get('mode') != 'paper':
            return False
        if not bool(config.get('paper_use_live_option_quotes', True)):
            return False
        if not (config.get('dhan_access_token') and config.get('dhan_client_id')):
            return False
        # Never mix live option quotes with replay/synthetic testing.
        if bool(config.get('paper_replay_enabled', False)):
            return False
        if bool(config.get('bypass_market_hours', False)):
            return False
        return bool(is_market_open())

    async def _paper_upgrade_sim_position_to_live(self) -> bool:
        """Try switching a SIM_* paper position to a real option security_id.

        This is only attempted during market hours (when configured).
        Returns True if upgraded (and current_option_ltp was updated), else False.
        """
        if not self._paper_should_use_live_option_quotes():
            return False
        if not self.current_position:
            return False

        security_id = str(self.current_position.get('security_id') or '')
        if not security_id.startswith('SIM_'):
            return False

    # --- State machine helpers -------------------------------------------------
    def set_state(self, new_state: State):
        try:
            old = getattr(self, 'state', None)
            self.state = new_state
            logger.info("State transition %s -> %s", old, new_state)
        except Exception:
            logger.exception("Failed to set state")

    def _on_entry_signal_event(self, payload):
        # Called when an ENTRY_SIGNAL is published by strategies
        try:
            # ignore if already handling a trade
            if self.trade_context and self.trade_context.state not in (State.CLOSED, State.IDLE):
                logger.info("Ignoring ENTRY_SIGNAL, existing trade in state %s", getattr(self.trade_context.state, 'value', None))
                return

            self.set_state(State.SIGNAL_GENERATED)
            # preserve last signal for downstream logic
            self.last_signal = payload

            # Build TradeContext from payload
            pos_id = payload.get('pos_id') or payload.get('trade_id') or f"pos_{int(datetime.utcnow().timestamp())}"
            symbol = payload.get('symbol') or config.get('selected_index')
            side = payload.get('side') or 'BUY'
            # prefer explicit size; else compute from score/base_lot
            score = None
            try:
                score = float(payload.get('score')) if payload.get('score') is not None else None
            except Exception:
                score = None
            base_lot = int(config.get('base_lot', 1) or 1)
            size = payload.get('quantity') or base_lot
            if score:
                try:
                    multiplier = 1.0
                    if score >= 75:
                        multiplier = 1.5
                    elif score >= 65:
                        multiplier = 1.0
                    elif score >= 55:
                        multiplier = 0.5
                    else:
                        multiplier = 0.0
                    size = max(0, int(base_lot * multiplier))
                except Exception:
                    size = int(size)

            tc = TradeContext(pos_id=str(pos_id), symbol=str(symbol), side=str(side), size=int(size or 0), entry_score=score)

            # pre-execution risk checks
            if not self._risk_checks_approve(tc):
                logger.warning("Risk checks failed for trade %s; aborting", pos_id)
                return

            tc.state = State.ENTRY_PENDING
            self.trade_context = tc
            try:
                bot_state['trade_context'] = tc.to_dict()
            except Exception:
                pass
        except Exception:
            logger.exception("Error handling ENTRY_SIGNAL")


    def _on_order_filled_event(self, payload):
        try:
            if not payload:
                return
            # payload expected to contain pos_id/trade_id, filled_price, filled_qty, security_id, symbol
            trade_id = payload.get('pos_id') or payload.get('trade_id') or payload.get('db_id')
            filled_price = payload.get('filled_price') or payload.get('price')
            filled_qty = int(payload.get('filled_qty') or payload.get('qty') or 0)
            security_id = payload.get('security_id') or ''
            symbol = payload.get('symbol') or config.get('selected_index')

            # Build current_position record (keep minimal keys used elsewhere)
            pos = {
                'trade_id': trade_id,
                'pos_id': trade_id,
                'security_id': str(security_id),
                'symbol': symbol,
                'qty': filled_qty,
            }

            # Update bot state
            self.current_position = pos
            try:
                bot_state['current_position'] = pos
            except Exception:
                pass

            if filled_price:
                try:
                    self.entry_price = float(filled_price)
                    bot_state['entry_price'] = float(filled_price)
                except Exception:
                    pass

            self.entry_time_utc = datetime.now(timezone.utc)
            try:
                bot_state['trading_enabled'] = bool(config.get('trading_enabled', True))
            except Exception:
                pass

            # Increment daily trade counter
            try:
                bot_state['daily_trades'] = int(bot_state.get('daily_trades', 0)) + 1
            except Exception:
                pass

            # Transition to POSITION_OPEN and update TradeContext
            try:
                # clear pending order if it matches
                try:
                    if self.pending_order and (str(self.pending_order.get('pos_id') or '') == str(trade_id) or str(self.pending_order.get('db_id') or '') == str(payload.get('db_id') or '')):
                        self.pending_order = None
                except Exception:
                    pass

                # If there's an active trade context, update it
                try:
                    if self.trade_context and (str(self.trade_context.pos_id) == str(trade_id) or str(self.trade_context.db_id) == str(payload.get('db_id') or '')):
                        self.trade_context.entry_price = float(filled_price) if filled_price else None
                        self.trade_context.entry_time = datetime.now(timezone.utc)
                        self.trade_context.state = State.POSITION_OPEN
                        try:
                            bot_state['trade_context'] = self.trade_context.to_dict()
                        except Exception:
                            pass
                except Exception:
                    pass

                self.set_state(State.POSITION_OPEN)
            except Exception:
                pass

            logger.info(f"[ORDER] ORDER_FILLED processed | Trade={trade_id} Sec={security_id} Price={filled_price} Qty={filled_qty}")
            # Slippage guard: if filled price deviates too much from expected
            try:
                expected = payload.get('price') or payload.get('expected_price')
                if expected and filled_price:
                    try:
                        exp = float(expected)
                        fp = float(filled_price)
                        slippage_pct = abs(fp - exp) / (abs(exp) if abs(exp) > 1e-6 else 1.0) * 100.0
                        max_slip = float(config.get('max_slippage_pct', 0.5) or 0.5)
                        if slippage_pct > max_slip:
                            logger.warning("Fill slippage %.2f%% > allowed %.2f%% for trade %s — triggering immediate exit", slippage_pct, max_slip, trade_id)
                            try:
                                publish('ORDER_SLIPPAGE', {'pos_id': trade_id, 'db_id': payload.get('db_id'), 'expected_price': exp, 'filled_price': fp, 'slippage_pct': slippage_pct})
                            except Exception:
                                pass
                            try:
                                publish('EXIT_SIGNAL', {'pos_id': trade_id, 'security_id': security_id, 'price': fp})
                            except Exception:
                                pass
                    except Exception:
                        pass
            except Exception:
                pass
            # If this fill corresponds to an EXIT for our trade context, finalize
            try:
                if self.trade_context and self.trade_context.state == State.EXIT_PENDING and (str(self.trade_context.pos_id) == str(trade_id) or str(self.trade_context.db_id) == str(payload.get('db_id') or '')):
                    # compute result and clear
                    try:
                        self._record_trade_result(self.trade_context, filled_price)
                    except Exception:
                        logger.exception('Error recording trade result on exit')
                    try:
                        self.trade_context.state = State.CLOSED
                        bot_state['trade_context'] = self.trade_context.to_dict()
                    except Exception:
                        pass
                    # clear current position
                    try:
                        self.current_position = None
                        bot_state['current_position'] = None
                    except Exception:
                        pass
                    # reset local context
                    try:
                        self.trade_context = None
                    except Exception:
                        pass
                    try:
                        self.set_state(State.CLOSED)
                    except Exception:
                        pass
            except Exception:
                pass
        except Exception:
            logger.exception("Error handling ORDER_FILLED event")


    def _on_order_timeout_event(self, payload):
        try:
            if not payload:
                return
            pos_id = payload.get('pos_id') or payload.get('trade_id') or payload.get('db_id')
            # If this corresponds to our active trade, abort and clear
            if self.trade_context and (str(self.trade_context.pos_id) == str(pos_id) or str(self.trade_context.db_id) == str(payload.get('db_id') or '')):
                logger.warning("Order timeout for trade %s — aborting trade_context", pos_id)
                # treat as failed attempt
                try:
                    bot_state['consecutive_losses'] = int(bot_state.get('consecutive_losses', 0)) + 1
                except Exception:
                    pass
                try:
                    self.trade_context.state = State.CLOSED
                    bot_state['trade_context'] = self.trade_context.to_dict()
                except Exception:
                    pass
                # clear local context and current position
                try:
                    self.trade_context = None
                except Exception:
                    pass
                try:
                    self.current_position = None
                    bot_state['current_position'] = None
                except Exception:
                    pass
                try:
                    self.set_state(State.CLOSED)
                except Exception:
                    pass
        except Exception:
            logger.exception('Error handling ORDER_TIMEOUT')


    def _on_order_placed_event(self, payload):
        """Handles ORDER_PLACED events to advance state to ORDER_PLACED and record pending order details."""
        try:
            if not payload:
                return
            trade_id = payload.get('pos_id') or payload.get('trade_id') or payload.get('db_id')
            db_id = payload.get('db_id')
            security_id = payload.get('security_id') or payload.get('symbol') or ''
            qty = int(payload.get('qty') or payload.get('quantity') or 0)
            price = payload.get('price') or payload.get('filled_price') or None

            self.pending_order = {'pos_id': trade_id, 'db_id': db_id, 'security_id': str(security_id), 'qty': qty, 'price': price}
            # attach to trade_context if present
            try:
                if self.trade_context and (str(self.trade_context.pos_id) == str(trade_id) or str(self.trade_context.db_id) == str(db_id)):
                    self.trade_context.db_id = db_id
                    self.trade_context.state = State.ORDER_PLACED
                    try:
                        bot_state['trade_context'] = self.trade_context.to_dict()
                    except Exception:
                        pass
            except Exception:
                pass
            try:
                self.last_order_time_utc = datetime.now(timezone.utc)
            except Exception:
                pass
            try:
                self.set_state(State.ORDER_PLACED)
            except Exception:
                pass
            logger.info(f"[ORDER] ORDER_PLACED recorded | Trade={trade_id} DB={db_id} Sec={security_id} Qty={qty} Price={price}")
        except Exception:
            logger.exception("Error handling ORDER_PLACED event")

    def _on_exit_signal_event(self, payload):
        try:
            self.set_state(State.EXIT_PENDING)
            self.last_signal = payload
        except Exception:
            logger.exception("Error handling EXIT_SIGNAL")


        index_name = str(self.current_position.get('index_name') or config.get('selected_index') or 'NIFTY')
        strike = int(self.current_position.get('strike') or 0)
        option_type = str(self.current_position.get('option_type') or '')
        expiry = str(self.current_position.get('expiry') or '')
        if not (index_name and strike and option_type and expiry):
            return False

        try:
            provider = str(config.get('market_data_provider', 'dhan') or 'dhan').strip().lower()
            base_url = str(config.get('mds_base_url', '') or '').strip()
            live_security_id = None

            # If MDS is configured, do NOT initialize or call Dhan for market data.
            if provider == 'mds' and base_url:
                from mds_client import fetch_option_chain, fetch_quote
                oc_payload = await fetch_option_chain(base_url=base_url, symbol=index_name, expiry=expiry)
                try:
                    oc = oc_payload.get('oc') if isinstance(oc_payload, dict) else oc_payload
                    if isinstance(oc, dict):
                        for k, node in oc.items():
                            try:
                                kf = float(k)
                            except Exception:
                                continue
                            if int(round(kf)) == int(strike):
                                node_obj = node if isinstance(node, dict) else {}
                                target = node_obj.get('ce') if option_type == 'CE' else node_obj.get('pe')
                                if isinstance(target, dict):
                                    sid = target.get('security_id') or target.get('securityId')
                                    if sid:
                                        live_security_id = str(sid)
                                        break
                except Exception:
                    live_security_id = None
            else:
                # Only initialize Dhan when not using MDS as provider
                if not self.dhan:
                    try:
                        # Only initialize if provider allows direct Dhan usage
                        if provider != 'mds':
                            self.initialize_dhan()
                    except Exception:
                        pass
                if not self.dhan:
                    return False
                live_security_id = await self.dhan.get_atm_option_security_id(index_name, strike, option_type, expiry)

            # If we are exiting and have an active TradeContext, update state and handle result
            try:
                if self.trade_context and self.trade_context.state == State.POSITION_OPEN:
                    # mark exit pending
                    self.trade_context.state = State.EXIT_PENDING
                    try:
                        bot_state['trade_context'] = self.trade_context.to_dict()
                    except Exception:
                        pass
            except Exception:
                pass

        except Exception:
            logger.exception("Error handling EXIT_SIGNAL internals")

    # ---------------- Risk & State helpers ---------------------------------
    def _risk_checks_approve(self, tc: TradeContext) -> bool:
        # Enforce global trading_enabled
        if not bool(config.get('trading_enabled', True)) or not bool(bot_state.get('trading_enabled', True)):
            logger.warning("Trading disabled via config/state")
            return False

        # Daily absolute loss check
        daily_pnl = float(bot_state.get('daily_pnl', 0.0) or 0.0)
        daily_max_loss = float(config.get('daily_max_loss', 0) or 0)
        if daily_max_loss and daily_pnl <= -abs(daily_max_loss):
            logger.warning("Daily max loss exceeded (%.2f <= -%.2f)", daily_pnl, daily_max_loss)
            bot_state['daily_max_loss_triggered'] = True
            bot_state['trading_enabled'] = False
            return False

        # Consecutive losses check
        consecutive = int(bot_state.get('consecutive_losses', 0) or 0)
        limit = int(config.get('consecutive_losses_limit', 3) or 3)
        if limit and consecutive >= limit:
            logger.warning("Consecutive losses limit reached: %s", consecutive)
            bot_state['trading_enabled'] = False
            return False

        # Minimum size and basic checks
        if not tc.size or tc.size <= 0:
            logger.warning("Computed trade size is zero; skipping")
            return False

        return True

    def _record_trade_result(self, tc: TradeContext, exit_price: float):
        try:
            if not tc or tc.entry_price is None:
                return
            # crude pnl calc: (exit - entry) * qty; sign depends on side
            try:
                qty = int(tc.size or 0)
                if qty <= 0:
                    return
                entry = float(tc.entry_price)
                    pnl = (float(exit_price) - entry) * qty
                    if str(tc.side).upper() in ('SELL', 'SHORT'):
                        pnl = -pnl
                except Exception:
                    pnl = 0.0

                tc.pnl = float(pnl)
                # update bot_state totals
                try:
                    bot_state['daily_pnl'] = float(bot_state.get('daily_pnl', 0.0)) + pnl
                except Exception:
                    bot_state['daily_pnl'] = pnl

                # consecutive loss tracking
                try:
                    if pnl < 0:
                        bot_state['consecutive_losses'] = int(bot_state.get('consecutive_losses', 0)) + 1
                    else:
                        bot_state['consecutive_losses'] = 0
                except Exception:
                    pass

                # kill switch enforcement
                try:
                    daily_max_loss = float(config.get('daily_max_loss', 0) or 0)
                    if daily_max_loss and bot_state.get('daily_pnl', 0.0) <= -abs(daily_max_loss):
                        bot_state['daily_max_loss_triggered'] = True
                        bot_state['trading_enabled'] = False
                except Exception:
                    pass

                # persist summary
                try:
                    bot_state['last_trade_result'] = {'pos_id': tc.pos_id, 'pnl': tc.pnl}
                except Exception:
                    pass
            except Exception:
                logger.exception('Error recording trade result')

            if not live_security_id:
                return False

            # Fetch option LTP via MDS when configured, otherwise via Dhan
            if provider == 'mds' and base_url:
                q = await fetch_quote(base_url=base_url, symbol=f"SEC_{live_security_id}")
                option_ltp = None
                try:
                    option_ltp = float(q.get('ltp')) if isinstance(q, dict) and q.get('ltp') is not None else None
                except Exception:
                    option_ltp = None
            else:
                option_ltp = await self.dhan.get_option_ltp(
                    security_id=live_security_id,
                    strike=strike,
                    option_type=option_type,
                    expiry=expiry,
                    index_name=index_name,
                )
            if not option_ltp or float(option_ltp) <= 0:
                return False

            option_ltp = round(float(option_ltp) / 0.05) * 0.05
            option_ltp = round(float(option_ltp), 2)

            self.current_position['security_id'] = str(live_security_id)
            bot_state['current_position'] = self.current_position
            bot_state['current_option_ltp'] = option_ltp

            logger.info(
                f"[PAPER] SIM->LIVE quotes enabled | {index_name} {option_type} {strike} | SecID: {live_security_id} | LTP: {option_ltp}"
            )
            return True
        except Exception as e:
            logger.debug(f"[PAPER] SIM->LIVE upgrade failed: {e}")
            return False

    async def _init_paper_replay(self) -> None:
        """Load candle data from DB for after-hours paper replay."""
        try:
            index_name = config.get('selected_index', 'NIFTY')
            interval = int(config.get('candle_interval', 5) or 5)
            date_ist = str(config.get('paper_replay_date_ist', '') or '').strip() or None

            provider = str(config.get('market_data_provider', 'dhan') or 'dhan').strip().lower()
            base_url = str(config.get('mds_base_url', '') or '').strip()

            candles = []
            if provider == 'mds' and base_url and date_ist:
                # Consume-only replay: pull historical candles for the selected IST date
                # from market-data-service (TimescaleDB) and replay them locally.
                from mds_client import fetch_candles_for_ist_date

                candles = await fetch_candles_for_ist_date(
                    base_url=base_url,
                    symbol=index_name,
                    timeframe_seconds=interval,
                    date_ist=date_ist,
                    limit=200000,
                )
            else:
                # Legacy fallback: backend SQLite candle_data table (may be empty in consume-only setups)
                from database import get_candle_data_for_replay

                candles = await get_candle_data_for_replay(
                    index_name=index_name,
                    interval_seconds=interval,
                    date_ist=date_ist,
                    limit=20000,
                )

            self._paper_replay_candles = candles or []
            self._paper_replay_pos = 0
            self._paper_replay_htf_elapsed = 0

            src = "MDS" if (provider == 'mds' and base_url and date_ist) else "SQLITE"
            logger.info(f"[REPLAY] Loaded {len(self._paper_replay_candles)} candles | Source={src} | Index={index_name} Interval={interval}s DateIST={date_ist or 'latest'}")
        except Exception as e:
            self._paper_replay_candles = []
            self._paper_replay_pos = 0
            self._paper_replay_htf_elapsed = 0
            logger.error(f"[REPLAY] Failed to load candles: {e}")
    
    def _initialize_indicator(self):
        """Initialize indicators (SuperTrend + optional MACD confirmation)"""
        try:
            self.indicator = SuperTrend(
                period=config['supertrend_period'],
                multiplier=config['supertrend_multiplier']
            )
            # HTF indicator uses same parameters; timeframe aggregation handled in run_loop
            self.htf_indicator = SuperTrend(
                period=config['supertrend_period'],
                multiplier=config['supertrend_multiplier']
            )
            # MACD used for entry confirmation (if enabled)
            self.macd = MACD(
                fast=int(config.get('macd_fast', 12)),
                slow=int(config.get('macd_slow', 26)),
                signal=int(config.get('macd_signal', 9)),
            )

            self.adx = ADX(period=int(config.get('adx_period', 14) or 14))

            self.score_engine = ScoreEngine(
                st_period=int(config.get('supertrend_period', 7)),
                st_multiplier=float(config.get('supertrend_multiplier', 4)),
                macd_fast=int(config.get('macd_fast', 12)),
                macd_slow=int(config.get('macd_slow', 26)),
                macd_signal=int(config.get('macd_signal', 9)),
                base_timeframe_seconds=int(config.get('candle_interval', 5) or 5),
                bonus_macd_triple=float(config.get('mds_bonus_macd_triple', 1.0) or 0.0),
                bonus_macd_momentum=float(config.get('mds_bonus_macd_momentum', 0.5) or 0.0),
                bonus_macd_cross=float(config.get('mds_bonus_macd_cross', 0.5) or 0.0),
            )
            logger.info(f"[SIGNAL] SuperTrend initialized")

            self._initialize_strategy_runners()
        except Exception as e:
            logger.error(f"[ERROR] Failed to initialize indicator: {e}")
            # Fallback to SuperTrend
            self.indicator = SuperTrend(period=7, multiplier=4)
            self.htf_indicator = SuperTrend(period=7, multiplier=4)
            self.macd = MACD(fast=12, slow=26, signal=9)
            self.adx = ADX(period=int(config.get('adx_period', 14) or 14))
            self.score_engine = ScoreEngine(
                st_period=7,
                st_multiplier=4,
                macd_fast=12,
                macd_slow=26,
                macd_signal=9,
                base_timeframe_seconds=int(config.get('candle_interval', 5) or 5),
                bonus_macd_triple=float(config.get('mds_bonus_macd_triple', 1.0) or 0.0),
                bonus_macd_momentum=float(config.get('mds_bonus_macd_momentum', 0.5) or 0.0),
                bonus_macd_cross=float(config.get('mds_bonus_macd_cross', 0.5) or 0.0),
            )
            logger.info(f"[SIGNAL] SuperTrend (fallback) initialized")

            self._initialize_strategy_runners()

    def _initialize_strategy_runners(self) -> None:
        # Only ScoreMdsRunner is supported now.
        self._st_runner = None

        if self._mds_runner is None:
            self._mds_runner = ScoreMdsRunner()
        else:
            self._mds_runner.reset()

        self._strategy_runtime = build_strategy_runtime(config.get('indicator_type'))

    def _get_st_runner(self):
        # SuperTrend runners removed — return None
        return None

    def _get_mds_runner(self) -> ScoreMdsRunner:
        if self._mds_runner is None:
            self._mds_runner = ScoreMdsRunner()
        return self._mds_runner

    def _get_strategy_runtime(self):
        # Only ScoreMdsRuntime supported
        if self._strategy_runtime is None or not isinstance(self._strategy_runtime, ScoreMdsRuntime):
            self._strategy_runtime = build_strategy_runtime(config.get('indicator_type'))
        return self._strategy_runtime
    
    def reset_indicator(self):
        """Reset the selected indicator"""
        if self.indicator:
            self.indicator.reset()
        if self.htf_indicator:
            self.htf_indicator.reset()
        if self.macd:
            self.macd.reset()
        if self.adx:
            self.adx.reset()
        if self.score_engine:
            self.score_engine.reset()
        else:
            # If score_engine wasn't initialized for some reason, try to initialize now.
            try:
                self.score_engine = ScoreEngine(
                    st_period=int(config.get('supertrend_period', 7)),
                    st_multiplier=float(config.get('supertrend_multiplier', 4)),
                    macd_fast=int(config.get('macd_fast', 12)),
                    macd_slow=int(config.get('macd_slow', 26)),
                    macd_signal=int(config.get('macd_signal', 9)),
                    base_timeframe_seconds=int(config.get('candle_interval', 5) or 5),
                    bonus_macd_triple=float(config.get('mds_bonus_macd_triple', 1.0) or 0.0),
                    bonus_macd_momentum=float(config.get('mds_bonus_macd_momentum', 0.5) or 0.0),
                    bonus_macd_cross=float(config.get('mds_bonus_macd_cross', 0.5) or 0.0),
                )
            except Exception:
                self.score_engine = None

        if self._st_runner is not None:
            try:
                self._st_runner.reset()
            except Exception:
                pass
        if self._mds_runner is not None:
            try:
                self._mds_runner.reset()
            except Exception:
                pass
        self._strategy_runtime = build_strategy_runtime(config.get('indicator_type'))
        self._last_mds_candle_ts = None
        self._mds_htf_count = 0
        self._mds_htf_high = 0.0
        self._mds_htf_low = float('inf')
        self._mds_htf_close = 0.0
        logger.info(f"[SIGNAL] Indicator reset: {config.get('indicator_type', 'supertrend')}")

    def _log_st_entry_block(self, *, reason: str, signal: str, flipped: bool) -> None:
        reason = str(reason or '')
        if reason == 'no_flip':
            logger.info(f"[ENTRY] ✗ Skipping - No SuperTrend flip this candle | Signal={signal}")
            logger.info("[ENTRY_DECISION] NO | Reason=no_flip")
            return
        if reason == 'adx_not_ready':
            logger.info("[ENTRY] ✗ Skipping - ADX not ready yet")
            logger.info("[ENTRY_DECISION] NO | Reason=adx_not_ready")
            return
        if reason == 'adx_below_threshold':
            try:
                logger.info(
                    f"[ENTRY] ✗ Skipping - ADX below threshold | ADX={bot_state.get('adx_value', 0.0):.2f} < {float(config.get('adx_threshold', 25.0) or 25.0):.2f}"
                )
            except Exception:
                logger.info("[ENTRY] ✗ Skipping - ADX below threshold")
            logger.info("[ENTRY_DECISION] NO | Reason=adx_below_threshold")
            return
        if reason == 'macd_not_ready':
            logger.info("[ENTRY] ✗ Skipping - MACD not ready yet")
            logger.info("[ENTRY_DECISION] NO | Reason=macd_not_ready")
            return
        if reason == 'macd_not_confirming_buy':
            if self.macd:
                logger.info(
                    f"[ENTRY] ✗ Skipping - MACD not confirming BUY | MACD={self.macd.last_macd:.4f} SIG={self.macd.last_signal_line:.4f}"
                )
            logger.info("[ENTRY_DECISION] NO | Reason=macd_not_confirming_buy")
            return
        if reason == 'macd_not_confirming_sell':
            if self.macd:
                logger.info(
                    f"[ENTRY] ✗ Skipping - MACD not confirming SELL | MACD={self.macd.last_macd:.4f} SIG={self.macd.last_signal_line:.4f}"
                )
            logger.info("[ENTRY_DECISION] NO | Reason=macd_not_confirming_sell")
            return
        if reason == 'htf_not_ready':
            logger.info("[ENTRY] ✗ Skipping - HTF SuperTrend not ready yet (need 1m candles)")
            logger.info("[ENTRY_DECISION] NO | Reason=htf_not_ready")
            return
        if reason == 'htf_mismatch':
            htf_direction = getattr(self.htf_indicator, 'direction', 0) if self.htf_indicator else 0
            htf_side = 'GREEN' if htf_direction == 1 else 'RED'
            logger.info(f"[ENTRY] ✗ Skipping - HTF filter mismatch | LTF={signal}, HTF={htf_side}")
            logger.info("[ENTRY_DECISION] NO | Reason=htf_mismatch")
            return

        logger.info(f"[ENTRY_DECISION] NO | Reason={reason or 'blocked'}")
    
    def is_within_trading_hours(self) -> bool:
        """Check if current time allows new entries
        
        Returns:
            bool: True if within allowed trading hours, False otherwise
        """
        if config.get('bypass_market_hours', False) or (
            bot_state.get('mode') == 'paper' and config.get('paper_replay_enabled', False)
        ):
            return True

        ist = get_ist_time()
        current_time = ist.time()
        
        # Define trading hours
        NO_ENTRY_BEFORE = datetime.strptime("09:25", "%H:%M").time()  # No entry before 9:25 AM
        NO_ENTRY_AFTER = datetime.strptime("15:10", "%H:%M").time()   # No entry after 3:10 PM
        
        # Check if within allowed hours
        if current_time < NO_ENTRY_BEFORE:
            logger.info(f"[HOURS] Entry blocked - market not open yet (Current: {current_time.strftime('%H:%M')}, Opens: 09:25)")
            return False
        
        if current_time > NO_ENTRY_AFTER:
            logger.info(f"[HOURS] Entry blocked - market closing soon (Current: {current_time.strftime('%H:%M')}, Cutoff: 15:10)")
            return False
        
        logger.debug(f"[HOURS] Trading hours OK (Current: {current_time.strftime('%H:%M')})")
        return True
    
    async def start(self):
        """Start the trading bot"""
        if self.running:
            return {"status": "error", "message": "Bot already running"}
        
        # Only require Dhan in live mode. Paper mode can run without broker SDK.
        if bot_state.get('mode') != 'paper':
            if not self.initialize_dhan():
                return {"status": "error", "message": "Dhan API not available (credentials/SDK)"}
        else:
            # In paper mode, optionally initialize Dhan for live quotes if enabled
            # (e.g., when `paper_use_live_option_quotes` is True or provider is 'dhan').
            try:
                if bool(config.get('paper_use_live_option_quotes', True)) or str(config.get('market_data_provider', 'dhan')).strip().lower() == 'dhan':
                    self.initialize_dhan()
            except Exception:
                # Non-fatal: continue with paper flow even if Dhan init fails
                pass

        # Prepare replay candles for after-hours (or bypass-hours) paper simulation
        replay_enabled = bool(bot_state.get('mode') == 'paper' and config.get('paper_replay_enabled', False))
        if replay_enabled:
            await self._init_paper_replay()
            if not self._paper_replay_candles:
                return {"status": "error", "message": "Paper replay enabled but no candles found (MDS/DB)"}
        
        self.running = True
        bot_state['is_running'] = True
        self.reset_indicator()
        self.last_signal = None

        # Do not seed from "latest" history when running a dated replay.
        # Also skip MDS prefetch when running synthetic-only paper testing.
        synthetic_only = (
            bot_state.get('mode') == 'paper'
            and bool(config.get('bypass_market_hours', False))
            and not bool(config.get('paper_replay_enabled', False))
        )
        if (not replay_enabled) and (not synthetic_only):
            # Prefetch and seed indicators from Timescale via market-data-service so we don't
            # spend the first N candles "warming up" after each restart.
            await self._seed_indicators_from_mds_history()

        self.task = asyncio.create_task(self.run_loop())
        
        index_name = config['selected_index']
        interval = format_timeframe(config['candle_interval'])
        indicator_name = config.get('indicator_type', 'supertrend')
        logger.info(f"[BOT] Started - Index: {index_name}, Timeframe: {interval}, Indicator: {indicator_name}, Mode: {bot_state['mode']}")
        
        return {"status": "success", "message": f"Bot started for {index_name} ({interval})"}
    
    async def stop(self):
        """Stop the trading bot"""
        self.running = False
        bot_state['is_running'] = False
        if self.task:
            self.task.cancel()
        logger.info("[BOT] Stopped")
        return {"status": "success", "message": "Bot stopped"}
    
    async def squareoff(self):
        """Force square off current position"""
        if not self.current_position:
            return {"status": "error", "message": "No open position"}
        
        index_name = config['selected_index']
        qty = int(self.current_position.get('qty') or 0)
        if qty <= 0:
            index_config = get_index_config(index_name)
            qty = config['order_qty'] * index_config['lot_size']
        
        logger.info(f"[ORDER] Force squareoff initiated for {index_name}")
        
        exit_price = bot_state['current_option_ltp']
        pnl = (exit_price - self.entry_price) * qty
        closed = await self.close_position(exit_price, pnl, "Force Square-off")
        if closed:
            suffix = "(Paper)" if bot_state['mode'] == 'paper' else ""
            return {"status": "success", "message": f"Position squared off {suffix}. PnL: {pnl:.2f}"}
        return {"status": "error", "message": "Squareoff order not filled (position still open)"}
    
    async def close_position(self, exit_price: float, pnl: float, reason: str) -> bool:
        """Close current position and save trade"""
        if not self.current_position:
            return False
        
        trade_id = self.current_position.get('trade_id', '')
        index_name = self.current_position.get('index_name', config['selected_index'])
        option_type = self.current_position.get('option_type', '')
        strike = self.current_position.get('strike', 0)
        security_id = self.current_position.get('security_id', '')
        qty = int(self.current_position.get('qty') or 0)
        if qty <= 0:
            index_config = get_index_config(index_name)
            qty = config['order_qty'] * index_config['lot_size']
        
        exit_order_placed = False
        filled_exit_price = exit_price

        if bot_state['mode'] != 'paper' and security_id:
            # Delegate exit order placement to the execution layer via event bus
            try:
                from event_bus import publish
                payload = {
                    'pos_id': trade_id,
                    'security_id': security_id,
                    'symbol': index_name,
                    'qty': qty,
                    'price': exit_price,
                }
                publish('EXIT_SIGNAL', payload)
                exit_order_placed = True
                # Track last order timestamp (treat as exit action)
                self.last_order_time_utc = datetime.now(timezone.utc)
                logger.info("[ORDER] Published EXIT_SIGNAL for %s qty=%s", security_id, qty)
            except Exception as e:
                logger.error(f"[ORDER] ✗ Error publishing EXIT_SIGNAL: {e} | Trade: {trade_id}", exc_info=True)
                return False
        elif not security_id:
            logger.warning(f"[WARNING] Cannot send exit order - security_id missing for {index_name} {option_type} | Trade: {trade_id}")
            logger.info(f"[EXIT] ✓ Position closed | {index_name} {option_type} {strike} | Reason: {reason} | PnL: {pnl} | Order Placed: False")
            # Update DB in background - don't wait
            asyncio.create_task(update_trade_exit(
                trade_id=trade_id,
                exit_time=datetime.now(timezone.utc).isoformat(),
                exit_price=exit_price,
                pnl=pnl,
                exit_reason=reason
            ))
            # Track last order timestamp (treated as an exit action even if no broker order)
            self.last_order_time_utc = datetime.now(timezone.utc)
        elif bot_state['mode'] == 'paper':
            logger.info(f"[ORDER] Paper mode - EXIT order not placed to Dhan (simulated) | Trade: {trade_id}")
            logger.info(f"[EXIT] ✓ Position closed | {index_name} {option_type} {strike} | Reason: {reason} | PnL: {pnl} | Order Placed: False")
            # Update DB in background - don't wait
            asyncio.create_task(update_trade_exit(
                trade_id=trade_id,
                exit_time=datetime.now(timezone.utc).isoformat(),
                exit_price=exit_price,
                pnl=pnl,
                exit_reason=reason
            ))
            # Track last order timestamp (paper exit)
            self.last_order_time_utc = datetime.now(timezone.utc)

        # If we reached here in LIVE mode, the exit is filled. Use filled price for P&L if available.
        if bot_state['mode'] != 'paper' and self.dhan and security_id:
            pnl = (filled_exit_price - self.entry_price) * qty

            asyncio.create_task(update_trade_exit(
                trade_id=trade_id,
                exit_time=datetime.now(timezone.utc).isoformat(),
                exit_price=filled_exit_price,
                pnl=pnl,
                exit_reason=reason
            ))
        
        # Update state
        bot_state['daily_pnl'] += pnl
        bot_state['current_position'] = None
        bot_state['trailing_sl'] = None
        bot_state['entry_price'] = 0
        
        if bot_state['daily_pnl'] < -config['daily_max_loss']:
            bot_state['daily_max_loss_triggered'] = True
            logger.warning(f"[EXIT] Daily max loss triggered! PnL: {bot_state['daily_pnl']:.2f}")
        
        if pnl < 0 and abs(pnl) > bot_state['max_drawdown']:
            bot_state['max_drawdown'] = abs(pnl)
        
        # Track the signal at exit - require signal change before next entry
        # If we exited CE position, last signal was GREEN
        # If we exited PE position, last signal was RED
        if option_type == 'CE':
            self.last_signal = 'GREEN'
        elif option_type == 'PE':
            self.last_signal = 'RED'
        
        self.current_position = None
        self.entry_price = 0
        self.trailing_sl = None
        self.highest_profit = 0
        self.entry_time_utc = None

        logger.info(f"[EXIT] ✓ Position closed | {index_name} {option_type} {strike} | Reason: {reason} | PnL: {pnl:.2f} | Order Placed: {exit_order_placed}")
        return True
    
    async def run_loop(self):
        """Main trading loop"""
        logger.info("[BOT] Trading loop started")
        candle_start_time = datetime.now()
        high, low, close = 0, float('inf'), 0
        candle_number = 0

        # Higher timeframe (HTF) candle aggregation (default: 1 minute)
        htf_candle_start_time = datetime.now()
        htf_high, htf_low, htf_close = 0, float('inf'), 0
        htf_candle_number = 0
        
        while self.running:
            try:
                index_name = config['selected_index']
                candle_interval = config['candle_interval']

                replay_enabled = (
                    bot_state.get('mode') == 'paper' and bool(config.get('paper_replay_enabled', False))
                )
                
                # Check daily reset (9:15 AM IST)
                ist = get_ist_time()
                if ist.hour == 9 and ist.minute == 15:
                    bot_state['daily_trades'] = 0
                    bot_state['daily_pnl'] = 0.0
                    bot_state['daily_max_loss_triggered'] = False
                    bot_state['max_drawdown'] = 0.0
                    self.last_exit_candle_time = None
                    self.last_trade_time = None
                    self.last_signal = None
                    candle_number = 0
                    htf_candle_number = 0
                    self.reset_indicator()
                    logger.info("[BOT] Daily reset at 9:15 AM")
                
                # Force square-off at 3:25 PM (skip during paper replay)
                if (not replay_enabled) and should_force_squareoff() and self.current_position:
                    logger.info("[EXIT] Auto squareoff at 3:25 PM")
                    await self.squareoff()

                # Check if trading is allowed (skip during paper replay)
                if (not replay_enabled) and (not is_market_open()):
                    await asyncio.sleep(5)
                    continue
                
                if bot_state['daily_max_loss_triggered'] and not self.current_position:
                    await asyncio.sleep(5)
                    continue
                
                # Paper replay: drive candles from DB (after-hours simulation)
                if replay_enabled:
                    if self._paper_replay_pos >= len(self._paper_replay_candles):
                        logger.info("[REPLAY] Completed candle replay")
                        self.running = False
                        bot_state['is_running'] = False
                        break

                    row = self._paper_replay_candles[self._paper_replay_pos]
                    self._paper_replay_pos += 1

                    try:
                        high = float(row.get('high') or 0.0)
                        low = float(row.get('low') or float('inf'))
                        close = float(row.get('close') or 0.0)
                    except Exception:
                        high, low, close = 0.0, float('inf'), 0.0

                    # Treat candle close price as current LTP
                    if close > 0:
                        bot_state['index_ltp'] = close

                    # Update HTF (replay-time based, not wall-clock)
                    if config.get('htf_filter_enabled', True) and int(candle_interval) < 60 and close > 0:
                        if close > htf_high:
                            htf_high = close
                        if close < htf_low:
                            htf_low = close
                        htf_close = close

                        htf_seconds = int(config.get('htf_filter_timeframe', 60) or 60)
                        if htf_seconds != 60:
                            htf_seconds = 60

                        self._paper_replay_htf_elapsed += int(candle_interval)
                        if self._paper_replay_htf_elapsed >= htf_seconds:
                            htf_candle_number += 1
                            if htf_high > 0 and htf_low < float('inf'):
                                htf_value, htf_signal = self.htf_indicator.add_candle(htf_high, htf_low, htf_close)
                                if htf_value:
                                    bot_state['htf_supertrend_value'] = htf_value if isinstance(htf_value, (int, float)) else str(htf_value)
                                    if htf_signal == "GREEN":
                                        bot_state['htf_signal_status'] = "buy"
                                    elif htf_signal == "RED":
                                        bot_state['htf_signal_status'] = "sell"
                                    else:
                                        bot_state['htf_signal_status'] = "waiting"
                                    if htf_signal:
                                        bot_state['htf_supertrend_signal'] = htf_signal

                            htf_high, htf_low, htf_close = 0, float('inf'), 0
                            self._paper_replay_htf_elapsed = 0

                    # Score/indicator update on every replay candle
                    candle_number += 1
                    if high > 0 and low < float('inf'):
                        indicator_value, signal = self.indicator.add_candle(high, low, close)
                        macd_value = 0.0
                        if self.macd:
                            macd_line, _macd_cross = self.macd.add_candle(high, low, close)
                            if macd_line is not None:
                                macd_value = float(macd_line)

                        mds_snapshot = None
                        if config.get('indicator_type') == 'score_mds' and self.score_engine:
                            try:
                                mds_snapshot = self.score_engine.on_base_candle(Candle(high=float(high), low=float(low), close=float(close)))
                                bot_state['mds_score'] = float(mds_snapshot.score)
                                bot_state['mds_slope'] = float(mds_snapshot.slope)
                                bot_state['mds_acceleration'] = float(mds_snapshot.acceleration)
                                bot_state['mds_stability'] = float(mds_snapshot.stability)
                                bot_state['mds_confidence'] = float(mds_snapshot.confidence)
                                bot_state['mds_is_choppy'] = bool(mds_snapshot.is_choppy)
                                bot_state['mds_direction'] = str(mds_snapshot.direction)
                            except Exception as e:
                                logger.error(f"[MDS] ScoreEngine update failed: {e}", exc_info=True)
                                mds_snapshot = None

                        if indicator_value:
                            bot_state['supertrend_value'] = indicator_value if isinstance(indicator_value, (int, float)) else str(indicator_value)
                            bot_state['macd_value'] = macd_value
                            if signal == "GREEN":
                                bot_state['signal_status'] = "buy"
                            elif signal == "RED":
                                bot_state['signal_status'] = "sell"
                            else:
                                bot_state['signal_status'] = "waiting"

                        # Candle-close trading logic
                        current_candle_time = datetime.now()
                        if self.current_position:
                            option_ltp = bot_state['current_option_ltp']
                            sl_hit = await self.check_trailing_sl_on_close(option_ltp)
                            if sl_hit:
                                self.last_exit_candle_time = current_candle_time

                        runtime = self._get_strategy_runtime()
                        await runtime.on_closed_candle(
                            self,
                            ClosedCandleContext(
                                candle_interval_seconds=int(candle_interval or 0),
                                current_candle_time=current_candle_time,
                                close=float(close),
                                signal=str(signal or '') if signal else None,
                                mds_snapshot=mds_snapshot,
                                enforce_recent_exit_cooldown=False,
                                require_signal=True,
                            ),
                        )

                    # Update simulated option pricing if needed
                    if self.current_position:
                        security_id = self.current_position.get('security_id', '')
                        if security_id.startswith('SIM_'):
                            strike = self.current_position.get('strike', 0)
                            option_type = self.current_position.get('option_type', '')
                            index_ltp = bot_state['index_ltp']
                            if strike and index_ltp:
                                distance_from_atm = abs(index_ltp - strike)
                                if option_type == 'CE':
                                    intrinsic = max(0, index_ltp - strike)
                                else:
                                    intrinsic = max(0, strike - index_ltp)
                                atm_time_value = 150
                                time_decay_factor = max(0, 1 - (distance_from_atm / 500))
                                time_value = atm_time_value * time_decay_factor
                                simulated_ltp = intrinsic + time_value
                                tick_movement = random.choice([-0.10, -0.05, 0, 0.05, 0.10])
                                simulated_ltp += tick_movement
                                simulated_ltp = round(simulated_ltp / 0.05) * 0.05
                                simulated_ltp = max(0.05, round(simulated_ltp, 2))
                                bot_state['current_option_ltp'] = simulated_ltp

                    await self.broadcast_state()
                    speed = float(config.get('paper_replay_speed', 10.0) or 10.0)
                    speed = max(0.1, min(100.0, speed))
                    await asyncio.sleep(max(0.05, float(candle_interval) / speed))
                    continue

                # Fetch market data (skip if MarketDataService is active)
                provider = str(config.get('market_data_provider', 'dhan') or 'dhan').strip().lower()

                # In paper mode, when bypass_market_hours is enabled and paper replay is OFF,
                # run fully synthetic candles even if docker env defaults provider to 'mds'.
                # This enables strategy testing outside market hours.
                if (
                    bot_state.get('mode') == 'paper'
                    and bool(config.get('bypass_market_hours', False))
                    and not replay_enabled
                ):
                    provider = 'synthetic'

                mds_new_candle = None
                mds_new_candle_ts = None

                # Prefer consuming candles from market-data-service (Timescale-backed)
                if provider == 'mds':
                    try:
                        from mds_client import fetch_latest_candle

                        base_url = str(config.get('mds_base_url', '') or '').strip()
                        poll_s = float(config.get('mds_poll_seconds', 1.0) or 1.0)
                        candle = await fetch_latest_candle(
                            base_url=base_url,
                            symbol=index_name,
                            timeframe_seconds=int(config.get('candle_interval', candle_interval) or candle_interval),
                            min_poll_seconds=poll_s,
                        )
                        if isinstance(candle, dict) and (candle.get('close') is not None):
                            try:
                                close_price = float(candle.get('close') or 0.0)
                            except Exception:
                                close_price = 0.0

                            if close_price > 0:
                                bot_state['index_ltp'] = float(close_price)

                                mds_new_candle_ts = str(candle.get('ts') or '') or None
                                if mds_new_candle_ts and mds_new_candle_ts != self._last_mds_candle_ts:
                                    mds_new_candle = candle
                                    self._last_mds_candle_ts = mds_new_candle_ts
                        elif self.dhan:
                            # If MDS has no candles yet, fall back to direct quote.
                            try:
                                index_ltp = await asyncio.to_thread(self.dhan.get_index_ltp, index_name)
                                if index_ltp and index_ltp > 0:
                                    bot_state['index_ltp'] = float(index_ltp)
                            except Exception:
                                pass
                    except Exception:
                        # If MDS is unavailable, fall back to any other provider.
                        pass

                    # Option LTP is only needed when a real position is open.
                    # MDS currently does not expose option quotes, so we use Dhan (if configured) in that case.
                    if self.dhan and self.current_position is not None:
                        security_id = self.current_position.get('security_id', '')
                        if security_id and not security_id.startswith('SIM_'):
                            try:
                                option_security_id = int(security_id)
                                _index_ltp, option_ltp = await asyncio.to_thread(
                                    self.dhan.get_index_and_option_ltp, index_name, option_security_id
                                )
                                if option_ltp and option_ltp > 0:
                                    option_ltp = round(option_ltp / 0.05) * 0.05
                                    bot_state['current_option_ltp'] = round(option_ltp, 2)
                            except Exception:
                                pass

                    # If we received a new candle from MDS, process it as the authoritative candle close.
                    if isinstance(mds_new_candle, dict):
                        try:
                            high = float(mds_new_candle.get('high') or 0.0)
                            low = float(mds_new_candle.get('low') or float('inf'))
                            close = float(mds_new_candle.get('close') or 0.0)
                        except Exception:
                            high, low, close = 0.0, float('inf'), 0.0

                        # Update HTF aggregation from base candles (fixed to 60s in current code).
                        interval = int(config.get('candle_interval', candle_interval) or candle_interval)
                        if bool(config.get('htf_filter_enabled', True)) and interval < 60 and (60 % max(1, interval) == 0) and close > 0:
                            multiple_1m = 60 // max(1, interval)
                            self._mds_htf_count += 1
                            self._mds_htf_high = max(self._mds_htf_high, high)
                            self._mds_htf_low = min(self._mds_htf_low, low)
                            self._mds_htf_close = close
                            if self._mds_htf_count >= multiple_1m:
                                htf_candle_number += 1
                                if self._mds_htf_high > 0 and self._mds_htf_low < float('inf'):
                                    htf_value, htf_signal = self.htf_indicator.add_candle(
                                        self._mds_htf_high, self._mds_htf_low, self._mds_htf_close
                                    )
                                    if htf_value:
                                        bot_state['htf_supertrend_value'] = htf_value if isinstance(htf_value, (int, float)) else str(htf_value)
                                        if htf_signal == 'GREEN':
                                            bot_state['htf_signal_status'] = 'buy'
                                        elif htf_signal == 'RED':
                                            bot_state['htf_signal_status'] = 'sell'
                                        else:
                                            bot_state['htf_signal_status'] = 'waiting'
                                        if htf_signal:
                                            bot_state['htf_supertrend_signal'] = htf_signal

                                self._mds_htf_count = 0
                                self._mds_htf_high = 0.0
                                self._mds_htf_low = float('inf')
                                self._mds_htf_close = 0.0

                        # Only advance candle number / trading decisions when the candle is valid.
                        if high > 0 and low < float('inf') and close > 0:
                            current_candle_time = datetime.now()
                            candle_number += 1
                            await self._handle_closed_candle(
                                index_name=index_name,
                                candle_number=candle_number,
                                candle_interval=int(config.get('candle_interval', candle_interval) or candle_interval),
                                high=high,
                                low=low,
                                close=close,
                                current_candle_time=current_candle_time,
                            )

                # Legacy mode: fetch quotes directly via Dhan
                elif self.dhan and not bot_state.get('market_data_service_active', False):
                    has_position = self.current_position is not None
                    option_security_id = None
                    
                    if has_position:
                        security_id = self.current_position.get('security_id', '')
                        if security_id and not security_id.startswith('SIM_'):
                            option_security_id = int(security_id)
                    
                    # Fetch Index + Option LTP (Dhan SDK is sync; run in thread)
                    if option_security_id:
                        index_ltp, option_ltp = await asyncio.to_thread(
                            self.dhan.get_index_and_option_ltp, index_name, option_security_id
                        )
                        if index_ltp > 0:
                            bot_state['index_ltp'] = index_ltp
                        if option_ltp > 0:
                            option_ltp = round(option_ltp / 0.05) * 0.05
                            bot_state['current_option_ltp'] = round(option_ltp, 2)
                    else:
                        index_ltp = await asyncio.to_thread(self.dhan.get_index_ltp, index_name)
                        if index_ltp > 0:
                            bot_state['index_ltp'] = index_ltp
                
                # Paper testing: simulate index movement when bypass_market_hours is enabled.
                # This keeps candles moving even if the API returns stale/flat prices outside market hours.
                if bot_state.get('mode') == 'paper' and config.get('bypass_market_hours', False):
                    if bot_state.get('simulated_base_price') is None:
                        # Prefer any existing index_ltp as starting point; else fall back to a realistic base.
                        if bot_state.get('index_ltp', 0) > 0:
                            bot_state['simulated_base_price'] = float(bot_state['index_ltp'])
                        else:
                            if index_name == 'NIFTY':
                                bot_state['simulated_base_price'] = 23500.0
                            elif index_name == 'BANKNIFTY':
                                bot_state['simulated_base_price'] = 51500.0
                            elif index_name == 'FINNIFTY':
                                bot_state['simulated_base_price'] = 22000.0
                            elif index_name == 'MIDCPNIFTY':
                                bot_state['simulated_base_price'] = 12500.0
                            else:
                                bot_state['simulated_base_price'] = 70000.0  # SENSEX

                    tick_change = random.choice([-15, -10, -5, -2, 0, 2, 5, 10, 15])
                    bot_state['simulated_base_price'] += tick_change
                    bot_state['index_ltp'] = round(bot_state['simulated_base_price'], 2)
                    logger.debug(f"[TEST] Simulated {index_name} LTP: {bot_state['index_ltp']}")
                
                # Update candle data
                index_ltp = bot_state['index_ltp']
                # If we're in paper mode and no live quote is available, simulate a base price
                # so indicators and ScoreEngine can warm up and UI shows activity.
                if provider != 'mds' and bot_state.get('mode') == 'paper' and (not index_ltp or float(index_ltp) <= 0):
                    if bot_state.get('simulated_base_price') is None:
                        # Prefer any existing index_ltp as starting point; else fall back to a realistic base.
                        if bot_state.get('index_ltp', 0) > 0:
                            bot_state['simulated_base_price'] = float(bot_state['index_ltp'])
                        else:
                            if index_name == 'NIFTY':
                                bot_state['simulated_base_price'] = 23500.0
                            elif index_name == 'BANKNIFTY':
                                bot_state['simulated_base_price'] = 51500.0
                            elif index_name == 'FINNIFTY':
                                bot_state['simulated_base_price'] = 22000.0
                            elif index_name == 'MIDCPNIFTY':
                                bot_state['simulated_base_price'] = 12500.0
                            else:
                                bot_state['simulated_base_price'] = 70000.0  # SENSEX
                    # Apply a small random tick to keep movement realistic
                    from random import choice
                    tick_change = choice([-15, -10, -5, -2, 0, 2, 5, 10, 15])
                    bot_state['simulated_base_price'] += tick_change
                    index_ltp = round(bot_state['simulated_base_price'], 2)
                    bot_state['index_ltp'] = index_ltp
                if provider != 'mds' and index_ltp > 0:
                    if index_ltp > high:
                        high = index_ltp
                    if index_ltp < low:
                        low = index_ltp
                    close = index_ltp

                    # Update HTF candle data (for sub-1m trading intervals)
                    if config.get('htf_filter_enabled', True) and config.get('candle_interval', 60) < 60:
                        if index_ltp > htf_high:
                            htf_high = index_ltp
                        if index_ltp < htf_low:
                            htf_low = index_ltp
                        htf_close = index_ltp

                # Check if HTF candle is complete
                if provider != 'mds' and config.get('htf_filter_enabled', True) and config.get('candle_interval', 60) < 60:
                    htf_seconds = int(config.get('htf_filter_timeframe', 60))
                    if htf_seconds != 60:
                        htf_seconds = 60

                    htf_elapsed = (datetime.now() - htf_candle_start_time).total_seconds()
                    if htf_elapsed >= htf_seconds:
                        htf_candle_number += 1
                        if htf_high > 0 and htf_low < float('inf'):
                            htf_value, htf_signal = self.htf_indicator.add_candle(htf_high, htf_low, htf_close)

                            if htf_value:
                                bot_state['htf_supertrend_value'] = htf_value if isinstance(htf_value, (int, float)) else str(htf_value)

                                if htf_signal == "GREEN":
                                    bot_state['htf_signal_status'] = "buy"
                                elif htf_signal == "RED":
                                    bot_state['htf_signal_status'] = "sell"
                                else:
                                    bot_state['htf_signal_status'] = "waiting"

                                if htf_signal:
                                    bot_state['htf_supertrend_signal'] = htf_signal

                                logger.info(
                                    f"[HTF CANDLE CLOSE #{htf_candle_number}] {index_name} | "
                                    f"H={htf_high:.2f} L={htf_low:.2f} C={htf_close:.2f} | "
                                    f"HTF_ST={htf_value:.2f} | "
                                    f"Signal={htf_signal or 'None'}"
                                )

                        htf_candle_start_time = datetime.now()
                        htf_high, htf_low, htf_close = 0, float('inf'), 0
                
                # Check SL/Target on EVERY TICK (responsive protection)
                if self.current_position and bot_state['current_option_ltp'] > 0:
                    option_ltp = bot_state['current_option_ltp']
                    tick_exit = await self.check_tick_sl(option_ltp)
                    if tick_exit:
                        # Position exited on tick, reset candle for next entry
                        candle_start_time = datetime.now()
                        high, low, close = 0, float('inf'), 0
                        candle_number = 0
                        await asyncio.sleep(1)
                        continue
                
                # Check if candle is complete
                elapsed = (datetime.now() - candle_start_time).total_seconds()
                if provider != 'mds' and elapsed >= candle_interval:
                    current_candle_time = datetime.now()
                    candle_number += 1

                    await self._handle_closed_candle(
                        index_name=index_name,
                        candle_number=candle_number,
                        candle_interval=int(config.get('candle_interval', candle_interval) or candle_interval),
                        high=high,
                        low=low,
                        close=close,
                        current_candle_time=current_candle_time,
                    )

                    # Reset candle for next period
                    candle_start_time = datetime.now()
                    high, low, close = 0, float('inf'), 0
                
                # Handle paper mode simulation
                if self.current_position:
                    security_id = self.current_position.get('security_id', '')
                    
                    if security_id.startswith('SIM_'):
                        upgraded = await self._paper_upgrade_sim_position_to_live()
                        if not upgraded:
                            strike = self.current_position.get('strike', 0)
                            option_type = self.current_position.get('option_type', '')
                            index_ltp = bot_state['index_ltp']

                            if strike and index_ltp:
                                distance_from_atm = abs(index_ltp - strike)

                                if option_type == 'CE':
                                    intrinsic = max(0, index_ltp - strike)
                                else:
                                    intrinsic = max(0, strike - index_ltp)

                                atm_time_value = 150
                                time_decay_factor = max(0, 1 - (distance_from_atm / 500))
                                time_value = atm_time_value * time_decay_factor

                                simulated_ltp = intrinsic + time_value
                                tick_movement = random.choice([-0.10, -0.05, 0, 0.05, 0.10])
                                simulated_ltp += tick_movement

                                simulated_ltp = round(simulated_ltp / 0.05) * 0.05
                                simulated_ltp = max(0.05, round(simulated_ltp, 2))

                                bot_state['current_option_ltp'] = simulated_ltp
                
                # Broadcast state update
                await self.broadcast_state()
                
                await asyncio.sleep(1)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"[ERROR] Trading loop exception: {e}")
                await asyncio.sleep(5)
    
    async def broadcast_state(self):
        """Broadcast current state to WebSocket clients"""
        from server import manager

        payload = {
            "type": "state_update",
            "data": {
                "index_ltp": bot_state['index_ltp'],
                "supertrend_signal": bot_state['last_supertrend_signal'],
                "supertrend_value": bot_state['supertrend_value'],
                "htf_supertrend_signal": bot_state.get('htf_supertrend_signal'),
                "htf_supertrend_value": bot_state.get('htf_supertrend_value', 0.0),
                "position": bot_state['current_position'],
                "entry_price": bot_state['entry_price'],
                "current_option_ltp": bot_state['current_option_ltp'],
                "trailing_sl": bot_state['trailing_sl'],
                "daily_pnl": bot_state['daily_pnl'],
                "daily_trades": bot_state['daily_trades'],
                "is_running": bot_state['is_running'],
                "mode": bot_state['mode'],
                "mds_score": bot_state.get('mds_score', 0.0),
                "mds_slope": bot_state.get('mds_slope', 0.0),
                "mds_acceleration": bot_state.get('mds_acceleration', 0.0),
                "mds_stability": bot_state.get('mds_stability', 0.0),
                "mds_confidence": bot_state.get('mds_confidence', 0.0),
                "mds_is_choppy": bot_state.get('mds_is_choppy', False),
                "mds_direction": bot_state.get('mds_direction', 'NONE'),
                "trading_enabled": bool(config.get('trading_enabled', True)),
                "selected_index": config['selected_index'],
                "candle_interval": config['candle_interval'],
                "htf_filter_enabled": bool(config.get('htf_filter_enabled', True)),
                "htf_filter_timeframe": int(config.get('htf_filter_timeframe', 60)),
                "timestamp": datetime.now(timezone.utc).isoformat()
            }
        }

        try:
            logger.info(f"[STATE] Broadcasting state_update: index_ltp={payload['data'].get('index_ltp')} selected_index={payload['data'].get('selected_index')} mds_score={payload['data'].get('mds_score')}")
            await manager.broadcast(payload)
            logger.debug("[STATE] state_update broadcast complete")
        except Exception as e:
            logger.exception(f"[STATE] Failed to broadcast state_update: {e}")

    async def process_mds_on_close(self, mds_snapshot, index_ltp: float) -> bool:
        """Process score-engine snapshot on candle close.

        Entry/exit are driven purely by MDS + safety gates.
        """
        exited = False
        index_name = config['selected_index']
        index_config = get_index_config(index_name)

        runner = self._get_mds_runner()

        # Exit logic first
        if self.current_position:
            position_type = self.current_position.get('option_type', '')
            qty = int(self.current_position.get('qty') or 0)
            if qty <= 0:
                qty = int(config.get('order_qty', 1)) * index_config['lot_size']

            # Respect min-hold to avoid churn
            if self._min_hold_active():
                return False

            # Use normalized entry/exit scores produced by ScoreEngine
            score = float(getattr(mds_snapshot, 'entry_score', 0.0) or 0.0)
            slope = float(getattr(mds_snapshot, 'slope', 0.0) or 0.0)

            # Use slow timeframe MACD+Histogram as a confirmation signal for exits.
            # This reduces churn exits caused by base-TF noise.
            slow_mom = 0.0
            try:
                tf_scores = getattr(mds_snapshot, 'tf_scores', {}) or {}
                if isinstance(tf_scores, dict) and tf_scores:
                    slow_tf = max(int(k) for k in tf_scores.keys())
                    slow = tf_scores.get(slow_tf)
                    slow_mom = float(getattr(slow, 'macd_score', 0.0) or 0.0) + float(getattr(slow, 'hist_score', 0.0) or 0.0)
            except Exception:
                slow_mom = 0.0

            # For exits prefer the exit_score (normalized inverse of entry)
            exit_score = float(getattr(mds_snapshot, 'exit_score', 0.0) or 0.0)
            exit_decision = runner.decide_exit(position_type=str(position_type or ''), score=exit_score, slope=slope, slow_mom=slow_mom)
            if exit_decision.should_exit:
                exit_price = bot_state['current_option_ltp']
                pnl = (exit_price - self.entry_price) * qty
                logger.warning(
                    f"[MDS] ✗ EXIT | {position_type} | Score={score:.2f} Slope={slope:.2f} SlowMom={slow_mom:.1f} | Reason={exit_decision.reason} | P&L=₹{pnl:.2f}"
                )
                closed = await self.close_position(exit_price, pnl, exit_decision.reason)
                exited = bool(closed)
                if closed:
                    self.last_signal = None

            return exited

        # No position: entry logic

        # Enforce order cooldown between any two orders
        if not self._can_place_new_entry_order():
            remaining = self._remaining_entry_cooldown()
            logger.info(f"[MDS] Entry blocked - Order cooldown active ({remaining:.1f}s remaining)")
            return False

        if not can_take_new_trade():
            logger.info("[ENTRY_DECISION] NO | Reason=after_cutoff (MDS)")
            return False

        if bot_state['daily_trades'] >= config['max_trades_per_day']:
            logger.info(f"[MDS] Max daily trades reached ({config['max_trades_per_day']})")
            return False

        # Check min_trade_gap protection (optional)
        min_gap = config.get('min_trade_gap', 0)
        if min_gap > 0 and self.last_trade_time:
            time_since_last = (datetime.now() - self.last_trade_time).total_seconds()
            if time_since_last < min_gap:
                logger.info(f"[ENTRY_DECISION] NO | Reason=min_trade_gap (MDS) ({time_since_last:.1f}s < {min_gap}s)")
                return False

        direction = str(getattr(mds_snapshot, 'direction', 'NONE') or 'NONE')
        # Use entry_score for entry decisions
        score = float(getattr(mds_snapshot, 'entry_score', 0.0) or 0.0)
        slope = float(getattr(mds_snapshot, 'slope', 0.0) or 0.0)
        confidence = float(getattr(mds_snapshot, 'confidence', 0.0) or 0.0)

        ready = bool(getattr(mds_snapshot, 'ready', False))

        # Ensure Dhan client is initialized if needed
        if not self.dhan:
            try:
                self.initialize_dhan()
            except Exception:
                pass

        used_live_quote = False
        entry_price = 0.0

        provider = str(config.get('market_data_provider', 'dhan') or 'dhan').strip().lower()
        base_url = str(config.get('mds_base_url', '') or '').strip()

        if provider == 'mds' and base_url:
            try:
                from mds_client import fetch_option_chain, fetch_quote

                oc_payload = await fetch_option_chain(base_url=base_url, symbol=index_name, expiry=expiry)
                sec_id = None
                try:
                    oc = oc_payload.get('oc') if isinstance(oc_payload, dict) else oc_payload
                    if isinstance(oc, dict):
                        for k, node in oc.items():
                            try:
                                kf = float(k)
                            except Exception:
                                continue
                            if int(round(kf)) == int(strike):
                                node_obj = node if isinstance(node, dict) else {}
                                target = node_obj.get('ce') if option_type == 'CE' else node_obj.get('pe')
                                if isinstance(target, dict):
                                    sid = target.get('security_id') or target.get('securityId')
                                    if sid:
                                        sec_id = str(sid)
                                        break
                        if not sec_id:
                            best_key = None
                            best_diff = None
                            for k, node in oc.items():
                                try:
                                    kf = float(k)
                                except Exception:
                                    continue
                                diff = abs(kf - float(strike))
                                if best_diff is None or diff < best_diff:
                                    best_diff = diff
                                    best_key = k
                            if best_key:
                                node = oc.get(best_key) or {}
                                target = node.get('ce') if option_type == 'CE' else node.get('pe')
                                if isinstance(target, dict):
                                    sid = target.get('security_id') or target.get('securityId')
                                    if sid:
                                        sec_id = str(sid)
                except Exception:
                    sec_id = None

                if sec_id:
                    try:
                        q = await fetch_quote(base_url=base_url, symbol=f"SEC_{sec_id}")
                        if isinstance(q, dict) and q.get('quote'):
                            option_ltp = q['quote'].get('ltp')
                            if option_ltp and float(option_ltp) > 0:
                                entry_price = round(float(option_ltp) / 0.05) * 0.05
                                entry_price = round(float(entry_price), 2)
                                used_live_quote = True
                    except Exception:
                        used_live_quote = False
            except Exception as e:
                logger.debug(f"[ENTRY] PAPER live-quote failed: {e}")
        else:
            # Fallback to Dhan SDK for option LTP and security resolution
            try:
                security_id = await self.dhan.get_atm_option_security_id(index_name, strike, option_type, expiry)
                if security_id:
                    option_ltp = await self.dhan.get_option_ltp(
                        security_id=security_id,
                        strike=strike,
                        option_type=option_type,
                        expiry=expiry,
                        index_name=index_name,
                    )
                    if option_ltp and float(option_ltp) > 0:
                        entry_price = round(float(option_ltp) / 0.05) * 0.05
                        entry_price = round(float(entry_price), 2)
                        used_live_quote = True
            except Exception as e:
                logger.debug(f"[ENTRY] PAPER live-quote failed: {e}")
            if entry_decision.reason == 'score_too_low':
                logger.info(
                    f"[ENTRY_DECISION] NO | Reason=score_too_low (MDS) | Score={score:.2f} Slope={slope:.2f} Dir={direction}"
                )
                return False
            if entry_decision.reason == 'slope_too_low':
                logger.info(
                    f"[ENTRY_DECISION] NO | Reason=slope_too_low (MDS) | Score={score:.2f} Slope={slope:.2f} Dir={direction}"
                )
                return False
            if entry_decision.reason == 'arming':
                logger.info(
                    f"[MDS] Arming entry ({entry_decision.confirm_count}/{entry_decision.confirm_needed}) | "
                    f"Dir={direction} Score={score:.2f} Slope={slope:.2f} Conf={confidence:.2f}"
                )
            return False

        # Fixed lots: always use Settings value (order_qty). No confidence-based lot sizing.
        fixed_lots = int(config.get('order_qty', 1) or 1)

        option_type = entry_decision.option_type or ('CE' if direction == 'CE' else 'PE')
        atm_strike = round_to_strike(index_ltp, index_name)

        logger.info(
            f"[MDS] ENTRY | {option_type} | Score={score:.2f} Slope={slope:.2f} Conf={confidence:.2f} | "
            f"Lots={fixed_lots} | "
            f"Index={index_name} LTP={index_ltp:.2f} ATM={atm_strike}"
        )

        before = bot_state.get('current_position')
        await self.enter_position(option_type, atm_strike, index_ltp)
        after = bot_state.get('current_position')
        if before is None and after is not None:
            logger.info(f"[ENTRY_DECISION] YES | Confirmed (MDS) | {option_type} {atm_strike}")
        else:
            logger.info(f"[ENTRY_DECISION] NO | Entry blocked downstream (MDS) | {option_type} {atm_strike}")

        self.last_trade_time = datetime.now()
        runner.on_entry_attempted()
        return False
    
    async def check_trailing_sl(self, current_ltp: float):
        """Update SL values - initial fixed SL then trails profit using step-based method"""
        if not self.current_position:
            return
        
        # Check if trailing is completely disabled
        trail_start = config.get('trail_start_profit', 0)
        trail_step = config.get('trail_step', 0)
        
        if trail_start == 0 or trail_step == 0:
            # Trailing disabled - don't set any SL
            return
        
        profit_points = current_ltp - self.entry_price
        
        # Track highest profit reached
        if profit_points > self.highest_profit:
            self.highest_profit = profit_points
        
        # Step 1: Set initial fixed stoploss (if enabled)
        initial_sl = config.get('initial_stoploss', 0)
        if initial_sl > 0 and self.trailing_sl is None:
            self.trailing_sl = self.entry_price - initial_sl
            bot_state['trailing_sl'] = self.trailing_sl
            logger.info(f"[SL] Initial SL set: {self.trailing_sl:.2f} ({initial_sl} pts below entry)")
            return
        
        # Step 2: Start trailing SL after reaching trail_start_profit
        
        # Only start trailing after profit reaches trail_start_profit
        if profit_points < trail_start:
            return
        
        # Trailing behavior:
        # - Start trailing only after profit reaches trail_start
        # - Once active, SL trails behind highest profit by trail_step in step increments
        # Example: entry=100, start=10, step=5
        #   at profit=10 (LTP=110) -> SL locks +5 (105)
        #   at profit=15 (LTP=115) -> SL locks +10 (110)
        trail_levels = int((self.highest_profit - trail_start) / trail_step)
        locked_profit = (trail_start - trail_step) + (trail_levels * trail_step)
        locked_profit = max(0.0, locked_profit)
        new_sl = self.entry_price + locked_profit
        
        # Always move SL up, never down (protect profit)
        if self.trailing_sl is None or new_sl > self.trailing_sl:
            old_sl = self.trailing_sl
            self.trailing_sl = new_sl
            bot_state['trailing_sl'] = self.trailing_sl
            
            if old_sl and old_sl > (self.entry_price - initial_sl):
                # This is a trailing update (not initial trigger)
                logger.info(f"[SL] Trailing SL updated: {old_sl:.2f} → {new_sl:.2f} (Profit: {profit_points:.2f} pts)")
            else:
                # This is the first trailing activation
                logger.info(f"[SL] Trailing started: {new_sl:.2f} (Profit: {profit_points:.2f} pts)")

    
    async def check_trailing_sl_on_close(self, current_ltp: float) -> bool:
        """Check if trailing SL or target is hit on candle close"""
        if not self.current_position:
            return False
        
        index_config = get_index_config(config['selected_index'])
        qty = int(self.current_position.get('qty') or 0)
        if qty <= 0:
            qty = config['order_qty'] * index_config['lot_size']
        profit_points = current_ltp - self.entry_price
        
        # Check target first (if enabled)
        target_points = config.get('target_points', 0)
        if target_points > 0 and profit_points >= target_points:
            pnl = profit_points * qty
            logger.info(
                f"[EXIT] Target hit | LTP={current_ltp:.2f} | Entry={self.entry_price:.2f} | Profit={profit_points:.2f} pts | Target={target_points:.2f} pts"
            )
            closed = await self.close_position(current_ltp, pnl, "Target Hit")
            return bool(closed)
        
        # Update trailing SL
        await self.check_trailing_sl(current_ltp)
        
        # Check trailing SL
        if self.trailing_sl and current_ltp <= self.trailing_sl:
            pnl = (current_ltp - self.entry_price) * qty
            logger.info(f"[EXIT] Trailing SL hit | LTP={current_ltp:.2f} | SL={self.trailing_sl:.2f}")
            closed = await self.close_position(current_ltp, pnl, "Trailing SL Hit")
            return bool(closed)
        
        return False
    
    async def check_tick_sl(self, current_ltp: float) -> bool:
        """Check SL/Target on every tick (more responsive than candle close)"""
        if not self.current_position:
            return False
        
        index_config = get_index_config(config['selected_index'])
        qty = int(self.current_position.get('qty') or 0)
        if qty <= 0:
            qty = config['order_qty'] * index_config['lot_size']
        profit_points = current_ltp - self.entry_price
        pnl = profit_points * qty
        
        # Check DAILY max loss FIRST (highest priority)
        daily_max_loss = config.get('daily_max_loss', 0)
        if daily_max_loss > 0 and bot_state['daily_pnl'] + pnl < -daily_max_loss:
            logger.warning(
                f"[EXIT] ✗ Daily max loss BREACHED! | Current Daily P&L=₹{bot_state['daily_pnl']:.2f} | This trade P&L=₹{pnl:.2f} | Limit=₹{-daily_max_loss:.2f} | FORCE SQUAREOFF"
            )
            closed = await self.close_position(current_ltp, pnl, "Daily Max Loss")
            if closed:
                bot_state['daily_max_loss_triggered'] = True
            return bool(closed)
        
        # Check max loss per trade (if enabled)
        max_loss_per_trade = config.get('max_loss_per_trade', 0)
        if max_loss_per_trade > 0 and pnl < -max_loss_per_trade:
            logger.info(
                f"[EXIT] Max loss per trade hit | LTP={current_ltp:.2f} | Entry={self.entry_price:.2f} | Loss=₹{abs(pnl):.2f} | Limit=₹{max_loss_per_trade:.2f}"
            )
            closed = await self.close_position(current_ltp, pnl, "Max Loss Per Trade")
            return bool(closed)

        # Check target (if enabled)
        target_points = config.get('target_points', 0)
        if target_points > 0 and profit_points >= target_points:
            logger.info(
                f"[EXIT] Target hit (tick) | LTP={current_ltp:.2f} | Entry={self.entry_price:.2f} | Profit={profit_points:.2f} pts | Target={target_points:.2f} pts"
            )
            closed = await self.close_position(current_ltp, pnl, "Target Hit")
            return bool(closed)
        
        # Update trailing SL values
        await self.check_trailing_sl(current_ltp)
        
        # Check if trailing SL is breached
        if self.trailing_sl and current_ltp <= self.trailing_sl:
            pnl = (current_ltp - self.entry_price) * qty
            logger.info(f"[EXIT] Trailing SL hit (tick) | LTP={current_ltp:.2f} | SL={self.trailing_sl:.2f}")
            closed = await self.close_position(current_ltp, pnl, "Trailing SL Hit")
            return bool(closed)
        
        return False
    
    async def process_signal_on_close(self, signal: str, index_ltp: float, flipped: bool = False) -> bool:
        """Process SuperTrend signal on candle close"""
        # SuperTrend-based signal processing is removed; when using ScoreMds
        # the ScoreMdsRuntime will call `process_mds_on_close` instead.
        indicator_type = str(config.get('indicator_type', 'score_mds') or '').strip().lower()
        if indicator_type == 'score_mds':
            return False

        exited = False
        index_name = config['selected_index']
        index_config = get_index_config(index_name)
        qty = 0
        if self.current_position:
            qty = int(self.current_position.get('qty') or 0)
        if qty <= 0:
            qty = config['order_qty'] * index_config['lot_size']
        
        runner = self._get_st_runner()

        # Check for exit on SuperTrend direction reversal (PRIMARY exit trigger)
        # Exit based on SuperTrend direction change - this is the critical signal
        if self.current_position:
            position_type = self.current_position.get('option_type', '')
            # Get current SuperTrend direction from indicator
            st_direction = getattr(self.indicator, 'direction', 0)

            exit_decision = runner.decide_exit(
                position_type=str(position_type or ''),
                st_direction=int(st_direction or 0),
                min_hold_active=bool(self._min_hold_active()),
            )

            if exit_decision.should_exit:
                exit_price = bot_state['current_option_ltp']
                pnl = (exit_price - self.entry_price) * qty
                if position_type == 'CE':
                    logger.warning(f"[SIGNAL] ✗ REVERSAL: SuperTrend flipped RED - Exiting CE position IMMEDIATELY | P&L=₹{pnl:.2f}")
                else:
                    logger.warning(f"[SIGNAL] ✗ REVERSAL: SuperTrend flipped GREEN - Exiting PE position IMMEDIATELY | P&L=₹{pnl:.2f}")

                closed = await self.close_position(exit_price, pnl, exit_decision.reason)
                exited = bool(closed)
                if closed:
                    self.last_signal = None
        
        # Check if new trade allowed
        if self.current_position:
            logger.info("[ENTRY_DECISION] NO | Reason=position_open")
            return exited

        # Enforce order cooldown between any two orders (prevents exit->entry flip too fast)
        if not self._can_place_new_entry_order():
            remaining = self._remaining_entry_cooldown()
            logger.info(f"[ENTRY] ✗ Skipping - Order cooldown active ({remaining:.1f}s remaining)")
            logger.info("[ENTRY_DECISION] NO | Reason=order_cooldown")
            return exited
        
        if not can_take_new_trade():
            logger.info("[ENTRY_DECISION] NO | Reason=after_cutoff")
            return exited
        
        if bot_state['daily_trades'] >= config['max_trades_per_day']:
            logger.info(f"[SIGNAL] Max daily trades reached ({config['max_trades_per_day']})")
            logger.info("[ENTRY_DECISION] NO | Reason=max_daily_trades")
            return exited
        
        # Check min_trade_gap protection (optional)
        min_gap = config.get('min_trade_gap', 0)
        if min_gap > 0 and self.last_trade_time:
            time_since_last = (datetime.now() - self.last_trade_time).total_seconds()
            if time_since_last < min_gap:
                logger.info(f"[ENTRY_DECISION] NO | Reason=min_trade_gap ({time_since_last:.1f}s < {min_gap}s)")
                return exited
        
        # Enter new position
        if not signal:
            logger.info("[ENTRY_DECISION] NO | Reason=no_signal")
            return exited

        adx_last = None
        try:
            if self.adx and getattr(self.adx, 'adx_values', None):
                adx_last = float(self.adx.adx_values[-1])
        except Exception:
            adx_last = None

        entry_decision = runner.decide_entry(
            signal=signal,
            flipped=bool(flipped),
            trade_only_on_flip=bool(config.get('trade_only_on_flip', False)),
            htf_filter_enabled=bool(config.get('htf_filter_enabled', True)),
            candle_interval_seconds=int(config.get('candle_interval', 60) or 60),
            htf_direction=int(getattr(self.htf_indicator, 'direction', 0) if self.htf_indicator else 0),
            macd_confirmation_enabled=bool(config.get('macd_confirmation_enabled', True)),
            macd_last=(float(self.macd.last_macd) if (self.macd and self.macd.last_macd is not None) else None),
            macd_signal_line=(float(self.macd.last_signal_line) if (self.macd and self.macd.last_signal_line is not None) else None),
            adx_value=adx_last,
            adx_threshold=float(config.get('adx_threshold', 25.0) or 25.0),
        )

        if not entry_decision.should_enter:
            self._log_st_entry_block(reason=entry_decision.reason, signal=signal, flipped=bool(flipped))
            return exited
        
        # NOTE: Previously we compared against last trade signal (self.last_signal).
        # That behavior is replaced by candle-level flip detection via the `flipped` flag.
        
        option_type = str(entry_decision.option_type or '').strip().upper() or ('PE' if signal == 'RED' else 'CE')
        atm_strike = round_to_strike(index_ltp, index_name)
        
        # Log signal details
        logger.info(
            f"[ENTRY] Taking {option_type} | {signal} Signal | "
            f"Index: {index_name} | "
            f"LTP: {index_ltp:.2f} | "
            f"ATM Strike: {atm_strike} | "
            f"SuperTrend: {bot_state['supertrend_value']:.2f}"
        )

        before = bot_state.get('current_position')
        await self.enter_position(option_type, atm_strike, index_ltp)
        after = bot_state.get('current_position')
        if before is None and after is not None:
            logger.info(f"[ENTRY_DECISION] YES | Confirmed | {option_type} {atm_strike}")
        else:
            logger.info(f"[ENTRY_DECISION] NO | Entry blocked downstream | {option_type} {atm_strike}")
        self.last_trade_time = datetime.now()
        
        return exited
    
    async def enter_position(self, option_type: str, strike: int, index_ltp: float, override_lots: int | None = None):
        """Enter a new position with market validation"""
        # Soft pause: keep bot running (prices/indicators/exits), but block new entries
        if not config.get('trading_enabled', True):
            now = datetime.now(timezone.utc)
            if (
                self._last_entries_paused_log_time is None
                or (now - self._last_entries_paused_log_time).total_seconds() >= 10
            ):
                logger.info(
                    f"[ENTRY] Skipped - Trading paused (trading_enabled=false) | Would take {option_type} {strike}"
                )
                self._last_entries_paused_log_time = now
            return

        # CRITICAL: Double-check market is open before entering
        if not is_market_open():
            logger.warning(f"[ENTRY] ✗ BLOCKED - Market is CLOSED | Cannot enter {option_type} position")
            return
        
        # CHECK: Trading hours protection
        if not self.is_within_trading_hours():
            logger.warning(f"[ENTRY] ✗ BLOCKED - Outside trading hours | Cannot enter {option_type} position")
            return

        # Enforce minimum cooldown between orders
        if not self._can_place_new_entry_order():
            remaining = self._remaining_entry_cooldown()
            logger.info(f"[ENTRY] ✗ BLOCKED - Order cooldown active ({remaining:.1f}s remaining)")
            return
        
        index_name = config['selected_index']
        index_config = get_index_config(index_name)

        lots = int(config.get('order_qty', 1) or 1)
        if override_lots is not None:
            lots = max(1, int(override_lots))

        # Fixed lots by default (order_qty). Risk-based lot reduction is opt-in.
        qty = lots * index_config['lot_size']

        if bool(config.get('enable_risk_based_lots', False)):
            risk_per_trade = float(config.get('risk_per_trade', 0) or 0)
            sl_points = float(config.get('initial_stoploss', 0) or 0)
            if risk_per_trade > 0 and sl_points > 0:
                max_lots = int(risk_per_trade / (sl_points * index_config['lot_size']))
                if max_lots < 1:
                    logger.warning(
                        f"[POSITION] ✗ BLOCKED - risk_per_trade too low for 1 lot | Risk=₹{risk_per_trade} SL={sl_points} LotSize={index_config['lot_size']}"
                    )
                    return
                new_lots = max(1, min(int(max_lots), int(lots)))
                if new_lots != lots:
                    lots = new_lots
                    qty = lots * index_config['lot_size']
                    logger.info(
                        f"[POSITION] Size adjusted for risk: {lots} lots ({qty} qty) (Risk: ₹{risk_per_trade}, SL: {sl_points}pts)"
                    )
        
        trade_id = f"T{datetime.now().strftime('%Y%m%d%H%M%S')}"
        
        # Get expiry
        expiry = await self.dhan.get_nearest_expiry(index_name) if self.dhan else None
        if not expiry:
            ist = get_ist_time()
            expiry_day = index_config['expiry_day']
            days_until_expiry = (expiry_day - ist.weekday()) % 7
            if days_until_expiry == 0 and ist.hour >= 15:
                days_until_expiry = 7
            expiry_date = ist + timedelta(days=days_until_expiry)
            expiry = expiry_date.strftime("%Y-%m-%d")
        
        entry_price = 0
        security_id = ""
        
        # Paper mode
        if bot_state['mode'] == 'paper':
            used_live_quote = False

            # When market is open (and configured), paper trades should use LIVE option quotes.
            # This never places an order; it only affects pricing.
            if self._paper_should_use_live_option_quotes():
                if not self.dhan:
                    try:
                        self.initialize_dhan()
                    except Exception:
                        pass

                if self.dhan:
                    try:
                        security_id = await self.dhan.get_atm_option_security_id(index_name, strike, option_type, expiry)
                        if security_id:
                            option_ltp = await self.dhan.get_option_ltp(
                                security_id=security_id,
                                strike=strike,
                                option_type=option_type,
                                expiry=expiry,
                                index_name=index_name,
                            )
                            if option_ltp and float(option_ltp) > 0:
                                entry_price = round(float(option_ltp) / 0.05) * 0.05
                                entry_price = round(float(entry_price), 2)
                                used_live_quote = True
                    except Exception as e:
                        logger.debug(f"[ENTRY] PAPER live-quote failed: {e}")

            # Fallback: fully synthetic option premium (SIM_*)
            if not used_live_quote:
                security_id = f"SIM_{index_name}_{strike}_{option_type}"
                if entry_price <= 0:
                    distance = abs(index_ltp - strike)
                    intrinsic = max(0, index_ltp - strike) if option_type == 'CE' else max(0, strike - index_ltp)
                    time_value = 150 * max(0, 1 - (distance / 500))
                    entry_price = round((intrinsic + time_value) / 0.05) * 0.05
                    entry_price = round(entry_price, 2)

            label = "PAPER(LIVE-QUOTE)" if used_live_quote else "PAPER(SYNTHETIC)"
            logger.info(
                f"[ENTRY] {label} | {index_name} {option_type} {strike} | Expiry: {expiry} | Price: {entry_price} | Qty: {qty} | SecID: {security_id}"
            )
        
        # Live mode
        else:
            if not self.dhan:
                # Mode can be switched to live while the bot is running.
                # Try a lazy init once before failing the trade.
                try:
                    if not self.initialize_dhan():
                        logger.error("[ERROR] Dhan API not initialized")
                        return
                except Exception:
                    logger.error("[ERROR] Dhan API not initialized")
                    return

            # Resolve instrument + a more accurate entry price from market data
            try:
                security_id = await self.dhan.get_atm_option_security_id(index_name, strike, option_type, expiry)
                if not security_id:
                    logger.error(f"[ERROR] Could not find security ID for {index_name} {strike} {option_type}")
                    return

                option_ltp = await self.dhan.get_option_ltp(
                    security_id=security_id,
                    strike=strike,
                    option_type=option_type,
                    expiry=expiry,
                    index_name=index_name
                )
                if option_ltp > 0:
                    entry_price = round(option_ltp / 0.05) * 0.05
                    entry_price = round(entry_price, 2)
            except Exception as e:
                logger.error("[ERROR] Failed to get entry price: %s", e)
                return
            
            # Delegate entry order placement to the execution layer via event bus
            try:
                from event_bus import publish
                payload = {
                    'pos_id': trade_id,
                    'symbol': index_name,
                    'security_id': security_id,
                    'strike': strike,
                    'option_type': option_type,
                    'expiry': expiry,
                    'qty': qty,
                    'price': entry_price,
                }
                publish('ENTRY_SIGNAL', payload)
                logger.info("[ORDER] Published ENTRY_SIGNAL %s", {k: payload.get(k) for k in ('pos_id','security_id','qty')})
                # Track last order timestamp (entry order)
                self.last_order_time_utc = datetime.now(timezone.utc)
                # Transition state to ORDER_PLACED
                try:
                    self.set_state(State.ORDER_PLACED)
                except Exception:
                    pass
            except Exception as e:
                logger.error("[ORDER] ✗ Error publishing ENTRY_SIGNAL: %s", e)
                return

        # Track last order timestamp (paper mode entry)
        if bot_state['mode'] == 'paper':
            self.last_order_time_utc = datetime.now(timezone.utc)
        
        # Save position
        self.current_position = {
            'trade_id': trade_id,
            'option_type': option_type,
            'strike': strike,
            'expiry': expiry,
            'security_id': security_id,
            'index_name': index_name,
            'qty': qty,
            'entry_time': datetime.now(timezone.utc).isoformat()
        }
        self.entry_price = entry_price
        self.trailing_sl = None
        self.highest_profit = 0
        self.entry_time_utc = datetime.now(timezone.utc)
        
        bot_state['current_position'] = self.current_position
        bot_state['entry_price'] = self.entry_price
        bot_state['daily_trades'] += 1
        bot_state['current_option_ltp'] = entry_price
        
        # ONLY set last_signal AFTER position is successfully confirmed open
        self.last_signal = option_type[0].upper() + 'E'  # 'CE' -> 'C', 'PE' -> 'P'
        self.last_signal = 'GREEN' if option_type == 'CE' else 'RED'
        
        # Save to database in background - don't wait for DB commit
        asyncio.create_task(save_trade({
            'trade_id': trade_id,
            'entry_time': datetime.now(timezone.utc).isoformat(),
            'option_type': option_type,
            'strike': strike,
            'expiry': expiry,
            'entry_price': self.entry_price,
            'qty': qty,
            'mode': bot_state['mode'],
            'index_name': index_name,
            'created_at': datetime.now(timezone.utc).isoformat()
        }))

