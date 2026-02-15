import os
import logging
from typing import Tuple, Optional

from config import bot_state, config
from position_manager import default_manager

logger = logging.getLogger(__name__)

MAX_POSITION = int(os.getenv("MAX_POSITION", str(int(config.get('max_position', 200) or 200))))
MAX_DAILY_LOSS = float(os.getenv("MAX_DAILY_LOSS", str(float(config.get('max_daily_loss', 5000) or 5000))))
MAX_TRADES_PER_DAY = int(os.getenv("MAX_TRADES_PER_DAY", str(int(config.get('max_trades_per_day', 20) or 20))))
BASE_QTY = int(os.getenv("BASE_QTY", str(int(config.get('base_qty', 50) or 50))))


def _projected_net_qty_after(side: str, qty: int) -> int:
    open_positions = default_manager.list_positions()
    net_qty = 0
    for p in open_positions.values():
        try:
            if p.get('side') == 'BUY':
                net_qty += int(p.get('quantity') or 0)
            else:
                net_qty -= int(p.get('quantity') or 0)
        except Exception:
            continue
    if str(side).upper() == 'BUY':
        return net_qty + qty
    return net_qty - qty


def check_risk(side: str, requested_qty: int, payload: Optional[dict] = None) -> Tuple[bool, int]:
    """Return (approved, qty_to_use).

    - Checks daily loss, trade count, position exposure.
    - Applies simple sizing via `confidence_score` and `BASE_QTY` if present.
    """
    try:
        # apply confidence-based sizing if provided
        qty = int(requested_qty or 0)
        confidence = None
        if payload:
            confidence = payload.get('confidence_score')
        if confidence is not None:
            try:
                conf = float(confidence)
                qty = max(1, int(BASE_QTY * conf))
            except Exception:
                pass

        # daily loss protection
        daily_pnl = float(bot_state.get('daily_pnl', 0) or 0)
        if daily_pnl <= -abs(MAX_DAILY_LOSS):
            logger.error('Daily loss limit breached: %s <= -%s', daily_pnl, MAX_DAILY_LOSS)
            return False, qty

        # trade count protection
        trade_count = int(bot_state.get('daily_trade_count', 0) or 0)
        if trade_count >= MAX_TRADES_PER_DAY:
            logger.warning('Max trades per day reached: %s >= %s', trade_count, MAX_TRADES_PER_DAY)
            return False, qty

        # position exposure protection
        projected = _projected_net_qty_after(side, qty)
        if abs(projected) > MAX_POSITION:
            logger.warning('Risk limit exceeded: projected %s > max %s', projected, MAX_POSITION)
            return False, qty

        # TODO: margin check (best-effort) - integrate broker API if available
        # For now assume sufficient margin
        return True, qty
    except Exception:
        logger.exception('Risk check failed; rejecting by default')
        return False, requested_qty
