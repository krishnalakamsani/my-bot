from __future__ import annotations

from dataclasses import dataclass
from collections import deque
from math import sqrt
from typing import Deque, Dict, Optional, Tuple
from copy import deepcopy

from indicators import SuperTrend, MACD


@dataclass(frozen=True)
class Candle:
    high: float
    low: float
    close: float


@dataclass
class TFIndicators:
    timeframe_seconds: int
    supertrend: SuperTrend
    macd: MACD

    # Previous values for slope/flip logic
    prev_macd: Optional[float] = None
    prev_hist: Optional[float] = None
    prev_st_dir: Optional[int] = None

    # Recent ST flips for chop/regime detection
    st_flip_history: Deque[int] = None  # 1 for flip, 0 for no flip

    def __post_init__(self):
        if self.st_flip_history is None:
            self.st_flip_history = deque(maxlen=6)


@dataclass(frozen=True)
class TFScore:
    timeframe_seconds: int
    macd_score: float
    hist_score: float
    st_score: float
    bonus_score: float
    raw_score: float
    weighted_score: float
    st_direction: int  # 1 bullish, -1 bearish, 0 unknown


@dataclass(frozen=True)
class MDSnapshot:
    score: float
    slope: float
    acceleration: float
    stability: float
    confidence: float
    is_choppy: bool
    direction: str  # 'CE' | 'PE' | 'NONE'
    tf_scores: Dict[int, TFScore]
    ready: bool
    ready_timeframes: Tuple[int, ...]


class ScoreEngine:
    """Deterministic multi-timeframe score engine.

    Updates on base candles matching the selected trading timeframe,
    aggregates to the next higher timeframe (selected + next), computes:
    - Market Direction Score (MDS)
    - slope/acceleration
    - stability (stddev)
    - chop detection
    - confidence (0..1)

    This is meant to be explainable and loggable.
    """

    _TF_CHAIN = (5, 15, 30, 60, 300, 900)

    # Indicator scoring constants (normalized by close)
    _NORM_EPS = 1e-12
    _MACD_FLAT_DIFF_NORM = 2e-6  # ~0.0002% of price per candle
    _HIST_NEAR_ZERO_NORM = 2e-6
    _HIST_EXPAND_THRESH_NORM = 4e-6

    def __init__(
        self,
        st_period: int,
        st_multiplier: float,
        macd_fast: int,
        macd_slow: int,
        macd_signal: int,
        base_timeframe_seconds: int = 5,
        bonus_macd_triple: float = 1.0,
        bonus_macd_momentum: float = 0.5,
        bonus_macd_cross: float = 0.5,
    ):
        self.base_tf = int(base_timeframe_seconds)
        if self.base_tf not in self._TF_CHAIN:
            raise ValueError(f"Unsupported base_timeframe_seconds: {self.base_tf}")

        self.timeframes = (self.base_tf, self._next_tf(self.base_tf))

        # Weights: selected TF + next TF only
        # Increase next-TF weight to give higher-timeframe more influence
        self.tf_weights = {
            self.timeframes[0]: 1.0,
            self.timeframes[1]: 3.0,
        }

        # Bonus score knobs (added on top of the base MACD/HIST/ST scoring).
        # These are symmetric (+ for bullish alignment, - for bearish alignment).
        self.bonus_macd_triple = float(bonus_macd_triple or 0.0)
        self.bonus_macd_momentum = float(bonus_macd_momentum or 0.0)
        self.bonus_macd_cross = float(bonus_macd_cross or 0.0)

        # Base per-timeframe raw max is 6.0 (= 2 MACD + 2 HIST + 2 ST).
        # Add bonus headroom so neutral-band / chop scaling stays consistent.
        self._max_tf_raw = 6.0 + max(0.0, self.bonus_macd_triple) + max(0.0, self.bonus_macd_momentum) + max(0.0, self.bonus_macd_cross)

        # Direction band scales with the maximum possible score.
        max_possible = self._max_tf_raw * sum(self.tf_weights.values())
        self.neutral_band = max(4.0, round(0.30 * max_possible, 2))

        # Chop window: target ~2 minutes worth of base candles (min 8)
        self.chop_window = max(8, int(round(120 / max(1, self.base_tf))))

        self._tfs: Dict[int, TFIndicators] = {}
        for tf in self.timeframes:
            self._tfs[tf] = TFIndicators(
                timeframe_seconds=tf,
                supertrend=SuperTrend(period=st_period, multiplier=st_multiplier),
                macd=MACD(fast=macd_fast, slow=macd_slow, signal=macd_signal),
            )

        self._agg_partial: Dict[int, dict] = {self.timeframes[1]: {}}
        self._score_history: Deque[float] = deque(maxlen=max(60, self.chop_window * 5))
        self._slope_history: Deque[float] = deque(maxlen=max(60, self.chop_window * 5))

        # EWMA smoothing for total score to reduce volatility. Alpha in (0,1]
        # Lower alpha => stronger smoothing. Default 0.4 provides moderate smoothing.
        self.score_smoothing_alpha = float(bonus_macd_cross or 0.4) if False else 0.4
        self._score_ewma: Optional[float] = None

        # Persist last computed TFScore per timeframe so higher-TF contribution remains
        # stable between its candle completions.
        self._last_scores: Dict[int, TFScore] = {}
        for tf in self.timeframes:
            self._last_scores[tf] = self._neutral_tf_score(tf)

    def _neutral_tf_score(self, tf: int) -> TFScore:
        w = self.tf_weights.get(tf, 1.0)
        return TFScore(tf, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0 * w, 0)

    def _next_tf(self, tf: int) -> int:
        chain = list(self._TF_CHAIN)
        if tf not in chain:
            raise ValueError(f"Unsupported timeframe: {tf}")
        idx = chain.index(tf)
        if idx >= len(chain) - 1:
            raise ValueError(f"No higher timeframe available for: {tf}")
        return int(chain[idx + 1])

    def reset(self):
        for tf_state in self._tfs.values():
            tf_state.supertrend.reset()
            tf_state.macd.reset()
            tf_state.prev_macd = None
            tf_state.prev_hist = None
            tf_state.prev_st_dir = None
            tf_state.st_flip_history.clear()

        self._agg_partial = {self.timeframes[1]: {}}
        self._score_history.clear()
        self._slope_history.clear()
        for tf in self.timeframes:
            self._last_scores[tf] = self._neutral_tf_score(tf)

    def on_base_candle(self, candle: Candle) -> MDSnapshot:
        """Consume a base candle (selected timeframe); returns latest snapshot."""
        if candle.close <= 0:
            return self._snapshot({}, ready=False)

        base_tf, next_tf = self.timeframes
        tf_scores: Dict[int, TFScore] = {}
        tf_scores[base_tf] = self._update_tf(base_tf, candle)

        completed = self._aggregate(next_tf, candle)
        if completed is not None:
            tf_scores[next_tf] = self._update_tf(next_tf, completed)
        else:
            # If next-TF candle not yet completed, attempt a non-mutating "peek"
            # using the partial aggregated values so the next timeframe contributes
            # a best-effort weighted score without mutating live indicator state.
            state = self._agg_partial.get(next_tf, {})
            if state and state.get("count", 0) > 0:
                partial = Candle(high=float(state["high"]), low=float(state["low"]), close=float(state["close"]))
                # Use a deepcopy of the TFIndicators to avoid mutating real state
                peek_state = deepcopy(self._tfs[next_tf])
                tf_scores[next_tf] = self._compute_tf_score_from_state(peek_state, next_tf, partial)

        # Compute total score using the freshest TF scores available: prefer
        # the scores computed this tick (including a partial peek), otherwise
        # fall back to the last persisted TF score.
        total_score = 0.0
        for tf in self.timeframes:
            total_score += (tf_scores.get(tf) or self._last_tf_score(tf)).weighted_score

        # Apply EWMA smoothing to reduce volatile tick-to-tick changes.
        alpha = max(0.0, min(1.0, getattr(self, 'score_smoothing_alpha', 0.4)))
        if self._score_ewma is None:
            smoothed_score = total_score
        else:
            smoothed_score = alpha * total_score + (1.0 - alpha) * self._score_ewma
        self._score_ewma = smoothed_score

        prev_score = self._score_history[-1] if self._score_history else None
        slope = 0.0 if prev_score is None else (smoothed_score - prev_score)
        prev_slope = self._slope_history[-1] if self._slope_history else None
        acceleration = 0.0 if prev_slope is None else (slope - prev_slope)

        # Use smoothed score when storing history and computing stability/slope/confidence
        self._score_history.append(smoothed_score)
        self._slope_history.append(slope)

        stability = self._stddev(list(self._score_history)[-self.chop_window:]) if len(self._score_history) >= 5 else 0.0
        is_choppy = self._detect_chop()

        confidence = self._confidence(smoothed_score, slope, stability, tf_scores, is_choppy)
        direction = self._direction(smoothed_score)

        ready_tfs = tuple(sorted(self._ready_timeframes()))
        ready = all(tf in ready_tfs for tf in self.timeframes)

        # Always expose latest known TF scores for both timeframes
        snapshot_tf_scores = {tf: self._last_tf_score(tf) for tf in self.timeframes}

        return MDSnapshot(
            score=round(smoothed_score, 3),
            slope=round(slope, 3),
            acceleration=round(acceleration, 3),
            stability=round(stability, 3),
            confidence=round(confidence, 3),
            is_choppy=bool(is_choppy),
            direction=direction,
            tf_scores={k: v for k, v in sorted(snapshot_tf_scores.items())},
            ready=bool(ready),
            ready_timeframes=ready_tfs,
        )

    def _aggregate(self, tf: int, candle: Candle) -> Optional[Candle]:
        if tf == self.base_tf:
            return candle

        if tf % self.base_tf != 0:
            raise ValueError(f"Cannot aggregate {self.base_tf}s -> {tf}s")

        multiple = tf // self.base_tf
        state = self._agg_partial[tf]

        if not state:
            state["count"] = 0
            state["high"] = candle.high
            state["low"] = candle.low
            state["close"] = candle.close

        state["count"] += 1
        state["high"] = max(state["high"], candle.high)
        state["low"] = min(state["low"], candle.low)
        state["close"] = candle.close

        if state["count"] >= multiple:
            completed = Candle(high=float(state["high"]), low=float(state["low"]), close=float(state["close"]))
            self._agg_partial[tf] = {}
            return completed
        return None

    def _last_tf_score(self, tf: int) -> TFScore:
        return self._last_scores.get(tf) or self._neutral_tf_score(tf)

    def _update_tf(self, tf: int, candle: Candle) -> TFScore:
        # Delegate core scoring to a pure helper that can operate on any TFIndicators
        # state (including deep-copies) so we can peek without mutating live state.
        state = self._tfs[tf]
        out = self._compute_tf_score_from_state(state, tf, candle)
        # Persist latest TF score when we actually update the real timeframe
        self._last_scores[tf] = out
        return out

    def _compute_tf_score_from_state(self, state: TFIndicators, tf: int, candle: Candle) -> TFScore:
        # This mirrors the logic from the original _update_tf but operates on the
        # provided `state` object and DOES NOT persist results to self._last_scores.
        close = candle.close

        st_value, st_signal = state.supertrend.add_candle(candle.high, candle.low, close)
        _macd_line, _macd_cross = state.macd.add_candle(candle.high, candle.low, close)

        # SuperTrend score
        st_dir = 0
        if st_signal == "GREEN":
            st_dir = 1
        elif st_signal == "RED":
            st_dir = -1

        flipped = False
        if st_dir != 0 and state.prev_st_dir is not None and st_dir != state.prev_st_dir:
            flipped = True

        state.st_flip_history.append(1 if flipped else 0)
        flip_count = sum(state.st_flip_history)

        if st_dir == 0:
            st_score = 0.0
        elif flip_count >= 2:
            st_score = 0.0
        elif flipped:
            st_score = 1.0 * st_dir
        else:
            st_score = 2.0 * st_dir

        state.prev_st_dir = st_dir if st_dir != 0 else state.prev_st_dir

        # MACD line score ("slow line" in spec)
        macd = state.macd.last_macd
        macd_score = 0.0
        if macd is not None:
            prev = state.prev_macd
            diff = 0.0 if prev is None else (macd - prev)
            diff_norm = diff / max(abs(close), self._NORM_EPS)
            rising = diff_norm > self._MACD_FLAT_DIFF_NORM
            falling = diff_norm < -self._MACD_FLAT_DIFF_NORM

            if abs(diff_norm) <= self._MACD_FLAT_DIFF_NORM:
                macd_score = 0.0
            else:
                if macd > 0 and rising:
                    macd_score = 2.0
                elif macd > 0 and falling:
                    macd_score = 1.0
                elif macd < 0 and falling:
                    macd_score = -2.0
                elif macd < 0 and rising:
                    macd_score = -1.0
                else:
                    macd_score = 0.0

            state.prev_macd = macd

        # Histogram score
        hist = state.macd.last_histogram
        hist_score = 0.0
        if hist is not None:
            prevh = state.prev_hist
            diffh = 0.0 if prevh is None else (hist - prevh)
            hist_norm = hist / max(abs(close), self._NORM_EPS)
            diff_norm = diffh / max(abs(close), self._NORM_EPS)

            if abs(hist_norm) <= self._HIST_NEAR_ZERO_NORM:
                hist_score = 0.0
            else:
                expanding = diff_norm > self._HIST_EXPAND_THRESH_NORM
                contracting = diff_norm < -self._HIST_EXPAND_THRESH_NORM

                if hist > 0 and expanding:
                    hist_score = 2.0
                elif hist > 0 and contracting:
                    hist_score = 1.0
                elif hist < 0 and contracting:
                    hist_score = -2.0
                elif hist < 0 and expanding:
                    hist_score = -1.0
                else:
                    hist_score = 0.0

            state.prev_hist = hist

        # Bonus scoring
        bonus = 0.0

        # 1) "All 3 MACD values" alignment bonus: MACD line, signal line, histogram
        #    - all > 0 => bullish bonus
        #    - all < 0 => bearish bonus
        # Use small normalized thresholds to avoid awarding bonus near zero.
        signal_line = state.macd.last_signal_line
        if macd is not None and signal_line is not None and hist is not None:
            macd_norm = macd / max(abs(close), self._NORM_EPS)
            sig_norm = signal_line / max(abs(close), self._NORM_EPS)
            hist_norm = hist / max(abs(close), self._NORM_EPS)

            macd_ok = abs(macd_norm) > self._MACD_FLAT_DIFF_NORM
            sig_ok = abs(sig_norm) > self._MACD_FLAT_DIFF_NORM
            hist_ok = abs(hist_norm) > self._HIST_NEAR_ZERO_NORM

            if macd_ok and sig_ok and hist_ok:
                if macd > 0 and signal_line > 0 and hist > 0:
                    bonus += self.bonus_macd_triple
                elif macd < 0 and signal_line < 0 and hist < 0:
                    bonus -= self.bonus_macd_triple

        # 2) Strong momentum bonus: when MACD + HIST both strongly agree (already scored),
        # add a small kicker to improve conviction during clean trends.
        if macd_score >= 2.0 and hist_score >= 2.0:
            bonus += self.bonus_macd_momentum
        elif macd_score <= -2.0 and hist_score <= -2.0:
            bonus -= self.bonus_macd_momentum

        # 3) Cross bonus: MACD crossing signal line often marks a regime shift.
        cross = state.macd.last_cross
        if cross == "GREEN":
            bonus += self.bonus_macd_cross
        elif cross == "RED":
            bonus -= self.bonus_macd_cross

        raw = macd_score + hist_score + st_score + bonus
        weight = self.tf_weights.get(tf, 1.0)
        weighted = raw * weight

        out = TFScore(
            timeframe_seconds=tf,
            macd_score=macd_score,
            hist_score=hist_score,
            st_score=st_score,
            bonus_score=bonus,
            raw_score=raw,
            weighted_score=weighted,
            st_direction=st_dir,
        )
        return out

    def _ready_timeframes(self) -> set[int]:
        ready = set()
        for tf, state in self._tfs.items():
            # SuperTrend readiness is implicit via period; MACD readiness via EMAs.
            st_ready = len(state.supertrend.candles) >= state.supertrend.period
            macd_ready = state.macd.last_macd is not None and state.macd.last_histogram is not None
            if st_ready and macd_ready:
                ready.add(tf)
        return ready

    def _detect_chop(self) -> bool:
        window = list(self._score_history)[-self.chop_window:]
        if len(window) < 8:
            return False

        # Sign-flip frequency
        flips = 0
        prev_sign = 0
        for s in window:
            sign = 1 if s > 0 else (-1 if s < 0 else 0)
            if prev_sign != 0 and sign != 0 and sign != prev_sign:
                flips += 1
            if sign != 0:
                prev_sign = sign

        stability = self._stddev(window)
        mean_abs = sum(abs(s) for s in window) / max(1, len(window))

        # Scale thresholds with score range (older engine assumed ~45 max score)
        max_possible = self._max_tf_raw * sum(self.tf_weights.values())
        scale = max(0.35, min(1.0, max_possible / 45.0))
        stab_hi = 7.5 * scale
        mean_abs_mid = 12.0 * scale
        mean_abs_low = 7.0 * scale
        stab_low = 3.5 * scale

        if flips >= 4:
            return True
        if stability >= stab_hi and mean_abs <= mean_abs_mid:
            return True
        if mean_abs <= mean_abs_low and stability >= stab_low:
            return True
        return False

    def _confidence(self, score: float, slope: float, stability: float, tf_scores: Dict[int, TFScore], is_choppy: bool) -> float:
        if is_choppy:
            return 0.0

        # Normalizations based on max possible score in this configuration.
        max_possible = 6.0 * sum(self.tf_weights.values())
        mag = min(1.0, abs(score) / max(1.0, 0.70 * max_possible))
        slp = min(1.0, abs(slope) / 8.0)

        # Weighted alignment ratio
        total_w = 0.0
        aligned_w = 0.0
        sign_total = 1 if score > 0 else (-1 if score < 0 else 0)
        for tf in self.timeframes:
            w = self.tf_weights.get(tf, 1.0)
            total_w += w
            sc = tf_scores.get(tf, self._last_tf_score(tf)).weighted_score
            sign_tf = 1 if sc > 0 else (-1 if sc < 0 else 0)
            if sign_total != 0 and sign_tf == sign_total:
                aligned_w += w
        alignment = 0.0 if total_w <= 0 else aligned_w / total_w

        # Stability score: lower stddev = higher confidence
        stability_score = 1.0 - min(1.0, stability / 10.0)

        confidence = 0.35 * mag + 0.25 * slp + 0.25 * alignment + 0.15 * stability_score
        return max(0.0, min(1.0, confidence))

    def _direction(self, score: float) -> str:
        if score >= self.neutral_band:
            return "CE"
        if score <= -self.neutral_band:
            return "PE"
        return "NONE"

    def _stddev(self, xs: list[float]) -> float:
        if not xs:
            return 0.0
        m = sum(xs) / len(xs)
        v = sum((x - m) ** 2 for x in xs) / len(xs)
        return sqrt(v)

    def _snapshot(self, tf_scores: Dict[int, TFScore], ready: bool) -> MDSnapshot:
        return MDSnapshot(
            score=0.0,
            slope=0.0,
            acceleration=0.0,
            stability=0.0,
            confidence=0.0,
            is_choppy=False,
            direction="NONE",
            tf_scores=tf_scores,
            ready=bool(ready),
            ready_timeframes=(),
        )
