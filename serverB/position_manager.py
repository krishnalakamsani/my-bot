import threading
import logging
from datetime import datetime
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)


class Position:
    def __init__(self, symbol: str, side: str, quantity: int, entry_price: float, security_id: Optional[str] = None):
        self.symbol = symbol
        # validate side
        if not isinstance(side, str) or side.upper() not in ("BUY", "SELL"):
            raise ValueError(f"Invalid side: {side}")
        self.side = side.upper()  # BUY or SELL
        self.quantity = int(quantity)
        self.entry_price = float(entry_price)
        self.security_id = security_id
        self.open_ts = datetime.utcnow()
        self.closed_ts = None
        self.exit_price = None
        self.pnl = 0.0
        self.trailing_sl = None
        # position status: OPEN / CLOSED
        self.status = "OPEN"
        self.tags: Dict[str, Any] = {}

    def to_dict(self):
        return {
            "symbol": self.symbol,
            "side": self.side,
            "quantity": self.quantity,
            "entry_price": self.entry_price,
            "security_id": self.security_id,
            "open_ts": self.open_ts.isoformat(),
            "closed_ts": self.closed_ts.isoformat() if self.closed_ts else None,
            "exit_price": self.exit_price,
            "pnl": self.pnl,
            "trailing_sl": self.trailing_sl,
            "status": getattr(self, 'status', None),
            "tags": self.tags,
        }


class PositionManager:
    def __init__(self):
        self._positions: Dict[str, Position] = {}
        self._lock = threading.RLock()

    def open_position(self, pos_id: str, symbol: str, side: str, quantity: int, entry_price: float, security_id: Optional[str] = None, trailing_sl: Optional[float] = None):
        with self._lock:
            # Protect against accidental multiple positions
            if self._positions:
                logger.error("Attempted to open new position while existing positions present; rejecting open for %s", pos_id)
                return None

            # Also prevent duplicate symbol opens
            if any((pos.symbol == symbol) for pos in self._positions.values()):
                logger.warning("Position already open for symbol %s; rejecting new position %s", symbol, pos_id)
                return None

            p = Position(symbol, side, quantity, entry_price, security_id)
            p.trailing_sl = trailing_sl
            p.status = "OPEN"
            self._positions[pos_id] = p
            logger.info("Opened position %s: %s", pos_id, p.to_dict())
            return p

    def close_position(self, pos_id: str, exit_price: float):
        with self._lock:
            p = self._positions.get(pos_id)
            if not p:
                logger.warning("Close requested for unknown position %s", pos_id)
                return None
            p.exit_price = float(exit_price)
            p.closed_ts = datetime.utcnow()
            p.pnl = self._compute_pnl(p)
            p.status = "CLOSED"
            logger.info("Closed position %s PnL=%.2f", pos_id, p.pnl)

            # REMOVE FROM ACTIVE POSITIONS to avoid duplicates and memory leaks
            try:
                del self._positions[pos_id]
            except KeyError:
                pass

            return p

    def update_market_price(self, pos_id: str, market_price: float):
        with self._lock:
            p = self._positions.get(pos_id)
            if not p:
                return None
            # Do not overwrite realized PnL for closed positions
            if p.closed_ts:
                return p
            # compute unrealized PnL
            if p.side.upper() == 'BUY':
                p.pnl = (market_price - p.entry_price) * p.quantity
            else:
                p.pnl = (p.entry_price - market_price) * p.quantity
            return p

    def get_position(self, pos_id: str):
        with self._lock:
            return self._positions.get(pos_id)

    def list_positions(self):
        with self._lock:
            # return snapshot (dict of dicts) for UI; callers needing live objects should use get_position()
            return {k: v.to_dict() for k, v in self._positions.items()}

    def has_open_position(self) -> bool:
        with self._lock:
            return len(self._positions) > 0

    def check_trailing_stop(self, pos_id: str, market_price: float) -> bool:
        with self._lock:
            p = self._positions.get(pos_id)
            if not p or p.trailing_sl is None:
                return False
            if p.side.upper() == 'BUY' and market_price <= p.trailing_sl:
                return True
            if p.side.upper() == 'SELL' and market_price >= p.trailing_sl:
                return True
            return False

    def detect_broker_mismatch(self, pos_id: str, broker_security_id: Optional[str]):
        with self._lock:
            p = self._positions.get(pos_id)
            if not p:
                return False
            if p.security_id and broker_security_id and str(p.security_id) != str(broker_security_id):
                logger.warning("Broker mismatch for %s: expected %s got %s", pos_id, p.security_id, broker_security_id)
                return True
            return False

    def _compute_pnl(self, p: Position) -> float:
        if p.exit_price is None:
            return p.pnl
        if p.side.upper() == 'BUY':
            return (p.exit_price - p.entry_price) * p.quantity
        else:
            return (p.entry_price - p.exit_price) * p.quantity


# module-level default manager
default_manager = PositionManager()
