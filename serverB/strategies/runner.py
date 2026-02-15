from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from .score_mds import decide_entry_mds, decide_exit_mds
from config import config


@dataclass(frozen=True)
class StrategyEntryDecision:
    should_enter: bool
    option_type: str = ""  # 'CE' | 'PE'
    reason: str = ""
    confirm_count: int = 0
    confirm_needed: int = 0


@dataclass(frozen=True)
class StrategyExitDecision:
    should_exit: bool
    reason: str = ""


class ScoreMdsRunner:
    """Decision-only runner for the MDS/ScoreEngine strategy.

    Owns the multi-candle confirmation state.
    """

    def __init__(self) -> None:
        self._last_direction: Optional[str] = None
        self._confirm_count: int = 0

    def reset(self) -> None:
        self._last_direction = None
        self._confirm_count = 0

    def on_entry_attempted(self) -> None:
        """Call after an entry attempt (success or blocked downstream)."""
        self._confirm_count = 0

    def decide_exit(self, *, position_type: str, score: float, slope: float, slow_mom: float) -> StrategyExitDecision:
        # score is expected to be normalized exit_score (0..1)
        d = decide_exit_mds(
            position_type=str(position_type or ""),
            score=float(score or 0.0),
            slope=float(slope or 0.0),
            slow_mom=float(slow_mom or 0.0),
        )
        return StrategyExitDecision(bool(d.should_exit), str(d.reason or ""))

    def decide_entry(
        self,
        *,
        ready: bool,
        is_choppy: bool,
        direction: str,
        score: float,
        slope: float,
        confirm_needed: int,
    ) -> StrategyEntryDecision:
        direction = str(direction or "NONE")

        if not ready:
            return StrategyEntryDecision(False, "", "mds_not_ready")
        if is_choppy:
            return StrategyEntryDecision(False, "", "mds_choppy")

        if direction == "NONE":
            self._last_direction = direction
            self._confirm_count = 0
            return StrategyEntryDecision(False, "", "neutral_band")

        # Use normalized thresholds from config (scores expected in 0..1)
        score_min = float(config.get('mds_entry_score_min', 0.25) or 0.25)
        slope_min = float(config.get('mds_entry_slope_min', 0.2) or 0.2)
        confirm_needed_cfg = int(config.get('mds_confirm_needed', 2) or 2)

        if float(score or 0.0) < float(score_min):
            self._last_direction = direction
            self._confirm_count = 0
            return StrategyEntryDecision(False, "", "score_too_low")

        if abs(float(slope or 0.0)) < float(slope_min):
            self._last_direction = direction
            self._confirm_count = 0
            return StrategyEntryDecision(False, "", "slope_too_low")

        if self._last_direction == direction:
            self._confirm_count += 1
        else:
            self._last_direction = direction
            self._confirm_count = 1

        d = decide_entry_mds(
            ready=bool(ready),
            is_choppy=bool(is_choppy),
            direction=direction,
            score=float(score or 0.0),
            slope=float(slope or 0.0),
            confirm_count=int(self._confirm_count),
            confirm_needed=int(confirm_needed_cfg or 0),
        )

        return StrategyEntryDecision(
            bool(d.should_enter),
            str(d.option_type or ""),
            str(d.reason or ""),
            confirm_count=int(self._confirm_count),
            confirm_needed=int(confirm_needed_cfg or 0),
        )
