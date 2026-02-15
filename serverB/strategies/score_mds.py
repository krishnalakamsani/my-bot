from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

# Read runtime config to allow A/B legacy vs tuned thresholds during replay
from config import config


@dataclass(frozen=True)
class ExitDecision:
    should_exit: bool
    reason: str = ""


@dataclass(frozen=True)
class EntryDecision:
    should_enter: bool
    option_type: str = ""  # 'CE' | 'PE'
    reason: str = ""


def decide_exit_mds(*, position_type: str, score: float, slope: float, slow_mom: float) -> ExitDecision:
    """Deterministic exits for the ScoreEngine strategy.

    Mirrors the existing rules in `TradingBot._handle_mds_signal`.
    """
    neutral = abs(score) <= 6.0

    should_exit = False
    reason = ""

    # Support legacy vs tuned thresholds via config flag for A/B testing
    legacy = bool(config.get('use_legacy_thresholds', False))

    if position_type == "CE":
        if legacy:
            # Legacy behavior (pre-tuning)
            if score <= -10.0:
                if slow_mom <= -1.0:
                    should_exit = True
                    reason = "MDS Reversal (slow confirm)"
            elif neutral:
                # Only exit for neutral when slow momentum is near-zero AND
                # the short-term slope does not indicate continued upward pressure.
                # This avoids exiting winners when the trend is still rising.
                if abs(slow_mom) <= 1.0 and slope <= 0.0:
                    should_exit = True
                    reason = "MDS Neutral (slow confirm)"
            elif slope <= -2.0 and score < 12.0:
                if slow_mom <= 0.0:
                    should_exit = True
                    reason = "MDS Momentum Loss (slow confirm)"
        else:
            # Tuned behavior (stricter exits)
            if score <= -12.0:
                if slow_mom <= -1.5:
                    should_exit = True
                    reason = "MDS Reversal (slow confirm)"
            elif neutral:
                # Tuned: require slope to be non-positive for CE neutral exit
                if abs(slow_mom) <= 0.5 and slope <= 0.0:
                    should_exit = True
                    reason = "MDS Neutral (slow confirm)"
            elif slope <= -2.5 and score < 12.0:
                if slow_mom <= -0.5:
                    should_exit = True
                    reason = "MDS Momentum Loss (slow confirm)"

    elif position_type == "PE":
        if legacy:
            # Legacy behavior (pre-tuning)
            if score >= 10.0:
                if slow_mom >= 1.0:
                    should_exit = True
                    reason = "MDS Reversal (slow confirm)"
            elif neutral:
                # For PE positions, require slope to be non-negative to confirm neutral exit
                if abs(slow_mom) <= 1.0 and slope >= 0.0:
                    should_exit = True
                    reason = "MDS Neutral (slow confirm)"
            elif slope >= 2.0 and score > -12.0:
                if slow_mom >= 0.0:
                    should_exit = True
                    reason = "MDS Momentum Loss (slow confirm)"
        else:
            # Tuned behavior (stricter exits)
            if score >= 12.0:
                if slow_mom >= 1.5:
                    should_exit = True
                    reason = "MDS Reversal (slow confirm)"
            elif neutral:
                if abs(slow_mom) <= 0.5 and slope >= 0.0:
                    should_exit = True
                    reason = "MDS Neutral (slow confirm)"
            elif slope >= 2.5 and score > -12.0:
                if slow_mom >= 0.5:
                    should_exit = True
                    reason = "MDS Momentum Loss (slow confirm)"

    return ExitDecision(should_exit, reason)


def decide_entry_mds(
    *,
    ready: bool,
    is_choppy: bool,
    direction: str,
    score: float,
    slope: float,
    confirm_count: int,
    confirm_needed: int,
) -> EntryDecision:
    if not ready:
        return EntryDecision(False, "", "mds_not_ready")

    if is_choppy:
        return EntryDecision(False, "", "mds_choppy")

    if direction == "NONE":
        return EntryDecision(False, "", "neutral_band")

    # Raise entry bar slightly to reduce entries into fragile moves
    if abs(score) < 12.0:
        return EntryDecision(False, "", "score_too_low")

    if abs(slope) < 1.5:
        return EntryDecision(False, "", "slope_too_low")


    if confirm_count < confirm_needed:
        return EntryDecision(False, "", "arming")

    option_type = "CE" if direction == "CE" else "PE"
    return EntryDecision(True, option_type, "")
