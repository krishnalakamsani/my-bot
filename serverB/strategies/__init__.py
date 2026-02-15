"""Trading strategies (entry/exit rules).

This package intentionally contains *rules* (how to trade) and composes
indicator calculations from `backend/indicators.py`.

Keep `indicators.py` calculation-only.

Register available strategies here so the rest of the backend can import them
by module path. ATR strategy is added as a lightweight example.
"""

# Register strategies
from . import atr_strategy  # noqa: F401
