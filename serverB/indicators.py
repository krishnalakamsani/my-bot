# Multiple Trading Indicators
import logging

logger = logging.getLogger(__name__)

class SuperTrend:
    def __init__(self, period=7, multiplier=4):
        self.period = period
        self.multiplier = multiplier
        self.candles = []
        self.atr_values = []
        self.supertrend_values = []
        self.direction = 1  # 1 = GREEN (bullish), -1 = RED (bearish)
    
    def reset(self):
        """Reset indicator state"""
        self.candles = []
        self.atr_values = []
        self.supertrend_values = []
        self.direction = 1
    
    def add_candle(self, high, low, close):
        """Add a new candle and calculate SuperTrend"""
        self.candles.append({'high': high, 'low': low, 'close': close})
        
        if len(self.candles) < self.period:
            return None, None
        
        # Calculate True Range
        tr = max(
            high - low,
            abs(high - self.candles[-2]['close']) if len(self.candles) > 1 else 0,
            abs(low - self.candles[-2]['close']) if len(self.candles) > 1 else 0
        )
        
        # Calculate ATR
        if len(self.atr_values) == 0:
            # Initial ATR is simple average of TR
            trs = []
            for i in range(max(0, len(self.candles) - self.period), len(self.candles)):
                if i > 0:
                    prev = self.candles[i-1]
                    curr = self.candles[i]
                    tr_val = max(
                        curr['high'] - curr['low'],
                        abs(curr['high'] - prev['close']),
                        abs(curr['low'] - prev['close'])
                    )
                else:
                    tr_val = self.candles[i]['high'] - self.candles[i]['low']
                trs.append(tr_val)
            atr = sum(trs) / len(trs) if trs else 0
        else:
            atr = (self.atr_values[-1] * (self.period - 1) + tr) / self.period
        
        self.atr_values.append(atr)
        
        # Calculate basic upper and lower bands
        hl2 = (high + low) / 2
        basic_upper = hl2 + (self.multiplier * atr)
        basic_lower = hl2 - (self.multiplier * atr)
        
        # Final bands calculation
        if len(self.supertrend_values) == 0:
            final_upper = basic_upper
            final_lower = basic_lower
        else:
            prev = self.supertrend_values[-1]
            prev_close = self.candles[-2]['close']
            
            final_lower = basic_lower if basic_lower > prev['lower'] or prev_close < prev['lower'] else prev['lower']
            final_upper = basic_upper if basic_upper < prev['upper'] or prev_close > prev['upper'] else prev['upper']
        
        # Direction
        if len(self.supertrend_values) == 0:
            direction = 1 if close > final_upper else -1
        else:
            prev = self.supertrend_values[-1]
            if prev['direction'] == 1:
                direction = -1 if close < final_lower else 1
            else:
                direction = 1 if close > final_upper else -1
        
        self.direction = direction
        supertrend_value = final_lower if direction == 1 else final_upper
        
        self.supertrend_values.append({
            'upper': final_upper,
            'lower': final_lower,
            'value': supertrend_value,
            'direction': direction
        })
        
        # Keep only last 100 values
        if len(self.candles) > 100:
            self.candles = self.candles[-100:]
            self.atr_values = self.atr_values[-100:]
            self.supertrend_values = self.supertrend_values[-100:]
        
        signal = "GREEN" if direction == 1 else "RED"
        return supertrend_value, signal

class RSI:
    """Relative Strength Index Indicator"""
    def __init__(self, period=14):
        self.period = period
        self.closes = []
        self.rsi_values = []
    
    def reset(self):
        self.closes = []
        self.rsi_values = []
    
    def add_candle(self, high, low, close):
        """Add candle and calculate RSI"""
        self.closes.append(close)
        
        if len(self.closes) < self.period + 1:
            return None, None
        
        # Calculate gains and losses
        gains = []
        losses = []
        for i in range(1, len(self.closes)):
            change = self.closes[i] - self.closes[i-1]
            gains.append(max(0, change))
            losses.append(max(0, -change))
        
        # Average gain and loss
        avg_gain = sum(gains[-self.period:]) / self.period if self.period > 0 else 0
        avg_loss = sum(losses[-self.period:]) / self.period if self.period > 0 else 0
        
        # RS and RSI
        rs = avg_gain / avg_loss if avg_loss > 0 else 0
        rsi = 100 - (100 / (1 + rs)) if rs > 0 else 50
        
        self.rsi_values.append(rsi)
        
        # Signal: GREEN if RSI < 30 (oversold), RED if RSI > 70 (overbought)
        if rsi < 30:
            signal = "GREEN"
        elif rsi > 70:
            signal = "RED"
        else:
            signal = None
        
        return rsi, signal


class MACD:
    """Moving Average Convergence Divergence"""
    def __init__(self, fast=12, slow=26, signal=9):
        self.fast = fast
        self.slow = slow
        self.signal_period = signal
        self.closes = []
        self.macd_values = []

        # Latest computed values (for entry confirmation & telemetry)
        self.last_macd = None
        self.last_signal_line = None
        self.last_histogram = None
        self.last_cross = None

        self._fast_ema = None
        self._slow_ema = None
        self._signal_ema = None

        self._fast_seed = []
        self._slow_seed = []
        self._signal_seed = []

        self._last_relation = None  # 1 bullish (macd>=signal), -1 bearish
    
    def reset(self):
        self.closes = []
        self.macd_values = []

        self.last_macd = None
        self.last_signal_line = None
        self.last_histogram = None
        self.last_cross = None

        self._fast_ema = None
        self._slow_ema = None
        self._signal_ema = None

        self._fast_seed = []
        self._slow_seed = []
        self._signal_seed = []

        self._last_relation = None
    
    def _update_ema(self, current_ema, value, period, seed_list):
        """Incremental EMA update with SMA seeding."""
        if period <= 0:
            return None

        alpha = 2 / (period + 1)
        if current_ema is None:
            seed_list.append(value)
            if len(seed_list) < period:
                return None
            if len(seed_list) == period:
                return sum(seed_list) / period
            # Should not normally exceed, but keep stable if it does
            return sum(seed_list[-period:]) / period
        return value * alpha + current_ema * (1 - alpha)
    
    def add_candle(self, high, low, close):
        """Add candle and calculate MACD"""
        self.closes.append(close)

        # Update fast/slow EMAs
        self._fast_ema = self._update_ema(self._fast_ema, close, self.fast, self._fast_seed)
        self._slow_ema = self._update_ema(self._slow_ema, close, self.slow, self._slow_seed)

        if self._fast_ema is None or self._slow_ema is None:
            self.last_macd = None
            self.last_signal_line = None
            self.last_histogram = None
            self.last_cross = None
            return None, None

        macd = self._fast_ema - self._slow_ema
        self.macd_values.append(macd)

        # Update signal line EMA over MACD values
        self._signal_ema = self._update_ema(self._signal_ema, macd, self.signal_period, self._signal_seed)

        cross = None
        histogram = None
        relation = None

        if self._signal_ema is not None:
            histogram = macd - self._signal_ema
            relation = 1 if macd >= self._signal_ema else -1
            if self._last_relation is not None and relation != self._last_relation:
                cross = "GREEN" if relation == 1 else "RED"
            self._last_relation = relation

        self.last_macd = macd
        self.last_signal_line = self._signal_ema
        self.last_histogram = histogram
        self.last_cross = cross

        return macd, cross


class MovingAverage:
    """Exponential Moving Average based entries"""
    def __init__(self, fast_period=5, slow_period=20):
        self.fast_period = fast_period
        self.slow_period = slow_period
        self.closes = []
        self.fast_emas = []
        self.slow_emas = []
    
    def reset(self):
        self.closes = []
        self.fast_emas = []
        self.slow_emas = []
    
    def _ema(self, values, period):
        """Calculate EMA"""
        if len(values) < period:
            return None
        
        alpha = 2 / (period + 1)
        ema = sum(values[-period:]) / period
        for val in values[-len(values)+period:]:
            ema = val * alpha + ema * (1 - alpha)
        return ema
    
    def add_candle(self, high, low, close):
        """Add candle and calculate moving averages"""
        self.closes.append(close)
        
        if len(self.closes) < self.slow_period:
            return None, None
        
        fast_ema = self._ema(self.closes, self.fast_period)
        slow_ema = self._ema(self.closes, self.slow_period)
        
        if fast_ema is None or slow_ema is None:
            return None, None
        
        self.fast_emas.append(fast_ema)
        self.slow_emas.append(slow_ema)
        
        # Signal: GREEN if fast > slow (uptrend), RED if fast < slow (downtrend)
        if fast_ema > slow_ema:
            signal = "GREEN"
        elif fast_ema < slow_ema:
            signal = "RED"
        else:
            signal = None
        
        return fast_ema, signal


class BollingerBands:
    """Bollinger Bands - Volatility based indicator"""
    def __init__(self, period=20, num_std=2):
        self.period = period
        self.num_std = num_std
        self.closes = []
        self.bands = []
    
    def reset(self):
        self.closes = []
        self.bands = []
    
    def add_candle(self, high, low, close):
        """Add candle and calculate Bollinger Bands"""
        self.closes.append(close)
        
        if len(self.closes) < self.period:
            return None, None
        
        # Calculate SMA and std dev
        sma = sum(self.closes[-self.period:]) / self.period
        variance = sum((c - sma) ** 2 for c in self.closes[-self.period:]) / self.period
        std_dev = variance ** 0.5
        
        upper = sma + (std_dev * self.num_std)
        lower = sma - (std_dev * self.num_std)
        
        self.bands.append({'upper': upper, 'lower': lower, 'middle': sma})
        
        # Signal: GREEN if close < lower (oversold), RED if close > upper (overbought)
        if close < lower:
            signal = "GREEN"
        elif close > upper:
            signal = "RED"
        else:
            signal = None
        
        return {'upper': upper, 'lower': lower, 'middle': sma}, signal


class Stochastic:
    """Stochastic Oscillator"""
    def __init__(self, k_period=14, d_period=3):
        self.k_period = k_period
        self.d_period = d_period
        self.highs = []
        self.lows = []
        self.closes = []
        self.k_values = []
    
    def reset(self):
        self.highs = []
        self.lows = []
        self.closes = []
        self.k_values = []
    
    def add_candle(self, high, low, close):
        """Add candle and calculate Stochastic"""
        self.highs.append(high)
        self.lows.append(low)
        self.closes.append(close)
        
        if len(self.closes) < self.k_period:
            return None, None
        
        # Calculate K%
        highest = max(self.highs[-self.k_period:])
        lowest = min(self.lows[-self.k_period:])
        
        k = ((close - lowest) / (highest - lowest) * 100) if (highest - lowest) > 0 else 50
        self.k_values.append(k)
        
        # Calculate D% (SMA of K)
        if len(self.k_values) < self.d_period:
            return k, None
        
        d = sum(self.k_values[-self.d_period:]) / self.d_period
        
        # Signal: GREEN if K < 20 (oversold), RED if K > 80 (overbought)
        if k < 20:
            signal = "GREEN"
        elif k > 80:
            signal = "RED"
        else:
            signal = None
        
        return k, signal


class ADX:
    """Average Directional Index - Trend Strength"""
    def __init__(self, period=14):
        self.period = period
        self.highs = []
        self.lows = []
        self.closes = []
        self.adx_values = []
    
    def reset(self):
        self.highs = []
        self.lows = []
        self.closes = []
        self.adx_values = []
    
    def add_candle(self, high, low, close):
        """Add candle and calculate ADX"""
        self.highs.append(high)
        self.lows.append(low)
        self.closes.append(close)
        
        if len(self.closes) < self.period + 1:
            return None, None
        
        # Calculate directional movements
        plus_dm = max(0, high - self.highs[-2]) if len(self.highs) > 1 else 0
        minus_dm = max(0, self.lows[-2] - low) if len(self.lows) > 1 else 0
        
        if plus_dm > minus_dm:
            minus_dm = 0
        elif minus_dm > plus_dm:
            plus_dm = 0
        
        # Calculate ATR
        tr = max(
            high - low,
            abs(high - self.closes[-2]) if len(self.closes) > 1 else 0,
            abs(low - self.closes[-2]) if len(self.closes) > 1 else 0
        )
        
        # Simple ADX calculation (simplified)
        if len(self.closes) >= self.period * 2:
            recent_high = max(self.highs[-self.period:])
            recent_low = min(self.lows[-self.period:])
            adx = abs(recent_high - recent_low) / (sum([max(self.highs[i] - self.lows[i], 
                                                              abs(self.highs[i] - self.closes[i-1]) if i > 0 else 0,
                                                              abs(self.lows[i] - self.closes[i-1]) if i > 0 else 0) 
                                                        for i in range(-self.period, 0)]) / self.period + 0.001) * 100
        else:
            adx = 50  # Default middle value
        
        self.adx_values.append(adx)
        
        # Signal: GREEN if ADX > 25 (strong uptrend), RED if ADX < 25 but trending down
        if adx > 25:
            signal = "GREEN"  # Strong trend
        else:
            signal = "RED"  # Weak trend
        
        return adx, signal