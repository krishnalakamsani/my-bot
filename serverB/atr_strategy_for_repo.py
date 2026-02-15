"""
Scaffold ATR-based strategy module compatible with trading-bot-v2-mon.

Drop this file into the repo under `backend/strategies/atr_strategy.py` and register
it in the strategies registry if needed. The functions below follow a simple
entry/exit decision pattern and are intended as a starting point for full
integration with the repo's `TradingBot` / runner interfaces.
"""
from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class EntryDecision:
    should_enter: bool
    reason: str = ""


@dataclass(frozen=True)
class ExitDecision:
    should_exit: bool
    reason: str = ""


def decide_entry(candles, atr, atr_multiplier=1.5):
    """Return EntryDecision. `candles` is list of dicts with open/high/low/close."""
    if len(candles) < 16 or atr is None:
        return EntryDecision(False, "insufficient data")
    recent = candles[-1]
    prev_high = max([c["high"] for c in candles[-16:-1]])
    prev_low = min([c["low"] for c in candles[-16:-1]])
    if recent["close"] > prev_high + atr_multiplier * atr:
        return EntryDecision(True, "atr_breakout_long")
    if recent["close"] < prev_low - atr_multiplier * atr:
        return EntryDecision(True, "atr_breakout_short")
    return EntryDecision(False, "no breakout")


def decide_exit(position, candles, atr, stop_multiplier=1.0):
    """Return ExitDecision. Basic ATR-based stop logic."""
    if position is None:
        return ExitDecision(False, "no position")
    entry_price = position.get("entry_price")
    if entry_price is None or atr is None:
        return ExitDecision(False, "insufficient info")
    last = candles[-1]
    if position.get("side") == "BUY":
        if last["low"] < entry_price - stop_multiplier * atr:
            return ExitDecision(True, "stop_hit")
    else:
        if last["high"] > entry_price + stop_multiplier * atr:
            return ExitDecision(True, "stop_hit")
    return ExitDecision(False, "no exit")
