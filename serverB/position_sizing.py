from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class SizingResult:
    confidence: float
    desired_lots: int
    risk_cap_lots: int
    final_lots: int


class PositionSizingAgent:
    """Maps confidence -> desired lots, then caps by risk-per-trade.

    This does not place orders; it only returns an integer lots count.
    """

    def __init__(self, max_lots: int):
        self.max_lots = max(1, int(max_lots))

    def confidence_to_lots(self, confidence: float) -> int:
        c = max(0.0, min(1.0, float(confidence)))

        # Table mapping (deterministic)
        if c < 0.35:
            return 0
        if c < 0.55:
            return 1
        if c < 0.70:
            return min(2, self.max_lots)
        if c < 0.85:
            return min(3, self.max_lots)
        return min(4, self.max_lots)

    def apply_risk_cap(self, desired_lots: int, risk_per_trade_rupees: float, sl_points: float, lot_size: int) -> int:
        if risk_per_trade_rupees <= 0 or sl_points <= 0 or lot_size <= 0:
            return max(0, int(desired_lots))

        # 1 point ~= ₹1 per qty for option premium moves (approx used elsewhere in bot)
        risk_cap = int(risk_per_trade_rupees // (sl_points * lot_size))
        return max(0, min(int(desired_lots), int(risk_cap)))

    def size(self, confidence: float, risk_per_trade_rupees: float, sl_points: float, lot_size: int) -> SizingResult:
        desired = min(self.max_lots, self.confidence_to_lots(confidence))
        risk_cap = int(risk_per_trade_rupees // (sl_points * lot_size)) if (risk_per_trade_rupees > 0 and sl_points > 0 and lot_size > 0) else desired
        final_lots = self.apply_risk_cap(desired, risk_per_trade_rupees, sl_points, lot_size)
        return SizingResult(confidence=float(confidence), desired_lots=int(desired), risk_cap_lots=int(risk_cap), final_lots=int(final_lots))
