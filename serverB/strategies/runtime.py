from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import logging
from typing import Any, Optional

from config import bot_state

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ClosedCandleContext:
    candle_interval_seconds: int
    current_candle_time: datetime
    close: float
    signal: Optional[str]
    mds_snapshot: Any = None
    enforce_recent_exit_cooldown: bool = True
    require_signal: bool = False


class StrategyRuntime:
    """Per-strategy candle-close trade flow.

    Keeps `TradingBot._handle_closed_candle` clean by owning:
    - recent-exit cooldown checks
    - deciding which TradingBot handler to call

    `TradingBot` remains the orchestrator for orders/positions.
    """

    async def on_closed_candle(self, bot: Any, ctx: ClosedCandleContext) -> None:  # pragma: no cover
        raise NotImplementedError

    @staticmethod
    def _recent_exit_cooldown_active(bot: Any, *, current_candle_time: datetime, candle_interval_seconds: int) -> bool:
        last_exit = getattr(bot, 'last_exit_candle_time', None)
        if not last_exit:
            return False
        try:
            elapsed = (current_candle_time - last_exit).total_seconds()
            return elapsed < float(candle_interval_seconds or 0)
        except Exception:
            return False


class SuperTrendRuntime(StrategyRuntime):
    async def on_closed_candle(self, bot: Any, ctx: ClosedCandleContext) -> None:
        if bool(ctx.require_signal) and not ctx.signal:
            return

        prev_signal = bot_state.get('last_supertrend_signal')
        signal = str(ctx.signal or '')
        flipped = bool(signal) and (prev_signal is None or signal != prev_signal)
        if signal:
            bot_state['last_supertrend_signal'] = signal

        if bool(ctx.enforce_recent_exit_cooldown) and self._recent_exit_cooldown_active(
            bot,
            current_candle_time=ctx.current_candle_time,
            candle_interval_seconds=ctx.candle_interval_seconds,
        ):
            logger.info("[ENTRY_DECISION] NO | Reason=recent_exit_cooldown")
            return

        exited = await bot.process_signal_on_close(signal, float(ctx.close), flipped=bool(flipped))
        if exited:
            bot.last_exit_candle_time = ctx.current_candle_time


class ScoreMdsRuntime(StrategyRuntime):
    async def on_closed_candle(self, bot: Any, ctx: ClosedCandleContext) -> None:
        if ctx.mds_snapshot is None:
            return

        if bool(ctx.enforce_recent_exit_cooldown) and self._recent_exit_cooldown_active(
            bot,
            current_candle_time=ctx.current_candle_time,
            candle_interval_seconds=ctx.candle_interval_seconds,
        ):
            return

        exited = await bot.process_mds_on_close(ctx.mds_snapshot, float(ctx.close))
        if exited:
            bot.last_exit_candle_time = ctx.current_candle_time


def build_strategy_runtime(indicator_type: Optional[str]) -> StrategyRuntime:
    # Always return the ScoreMds runtime — only score strategy is supported now.
    return ScoreMdsRuntime()
