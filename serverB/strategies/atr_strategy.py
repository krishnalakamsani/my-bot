"""ATR breakout strategy module.

This is a lightweight strategy module compatible with the repo's strategy
runner. It provides `decide_entry` and `decide_exit` functions as a starting
point — integrate into your runner or register the module where strategies
are discovered.
"""
from dataclasses import dataclass


@dataclass(frozen=True)
class EntryDecision:
    should_enter: bool
    option_type: str = ""
    reason: str = ""


@dataclass(frozen=True)
class ExitDecision:
    should_exit: bool
    reason: str = ""


def compute_atr(candles, period=14):
    import numpy as np
    import pandas as pd
    if len(candles) < period + 1:
        return None
    df = pd.DataFrame(candles)
    high = df['high'].values
    low = df['low'].values
    close = df['close'].values
    prev_close = np.roll(close, 1)
    tr = np.maximum.reduce([high - low, np.abs(high - prev_close), np.abs(low - prev_close)])
    tr[0] = high[0] - low[0]
    atr = pd.Series(tr).rolling(window=period).mean()
    return float(atr.iloc[-1])


def decide_entry(candles, atr_period=14, atr_multiplier=1.5):
    """Return EntryDecision or None"""
    if len(candles) < 16:
        return EntryDecision(False, reason="insufficient_data")
    atr = compute_atr(candles, period=atr_period)
    if atr is None:
        return EntryDecision(False, reason="insufficient_atr")
    recent = candles[-1]
    prev_high = max([c['high'] for c in candles[-16:-1]])
    prev_low = min([c['low'] for c in candles[-16:-1]])
    if recent['close'] > prev_high + atr_multiplier * atr:
        return EntryDecision(True, option_type='CE', reason='atr_breakout_long')
    if recent['close'] < prev_low - atr_multiplier * atr:
        return EntryDecision(True, option_type='PE', reason='atr_breakout_short')
    return EntryDecision(False, reason='no_breakout')


def decide_exit(position, candles, atr_period=14, stop_multiplier=1.0):
    if position is None:
        return ExitDecision(False, reason='no_position')
    atr = compute_atr(candles, period=atr_period)
    if atr is None:
        return ExitDecision(False, reason='insufficient_atr')
    last = candles[-1]
    entry_price = position.get('entry_price')
    side = position.get('side')
    if entry_price is None or side is None:
        return ExitDecision(False, reason='insufficient_position_info')
    if side == 'BUY' and last['low'] < entry_price - stop_multiplier * atr:
        return ExitDecision(True, reason='stop_hit')
    if side == 'SELL' and last['high'] > entry_price + stop_multiplier * atr:
        return ExitDecision(True, reason='stop_hit')
    return ExitDecision(False, reason='hold')
