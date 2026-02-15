import os
import json
import time
import logging
from datetime import datetime, timedelta

import redis
import pandas as pd
import numpy as np
from event_bus import publish
from risk import check_risk
from position_manager import default_manager
from datetime import timezone

# cooldown between trades (seconds)
MIN_TRADE_GAP = int(os.getenv('MIN_TRADE_GAP', '300'))
last_trade_time_utc = None

logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(levelname)s %(message)s")

REDIS_HOST = os.getenv("REDIS_HOST", "redis")
DHAN_CLIENT_ID = os.getenv("DHAN_CLIENT_ID")
DHAN_ACCESS_TOKEN = os.getenv("DHAN_ACCESS_TOKEN")
SECURITY_ID = os.getenv("SECURITY_ID", "REPLACE_WITH_REAL_SECURITY_ID")
LOT_SIZE = int(os.getenv("LOT_SIZE", "65"))
SIMULATE = os.getenv("SIMULATE", "true").lower() in ("1", "true", "yes")

# Strategy params
TIMEFRAMES = os.getenv("TIMEFRAMES", "1m,15m")  # base and higher TFs
ATR_PERIOD = int(os.getenv("ATR_PERIOD", "14"))
ATR_MULTIPLIER = float(os.getenv("ATR_MULTIPLIER", "1.5"))

def _read_secret_from_file(path):
    try:
        if path and os.path.exists(path):
            return open(path, "r").read().strip()
    except Exception:
        pass
    return None

REDIS_PASSWORD = os.getenv("REDIS_PASSWORD") or _read_secret_from_file(os.getenv("REDIS_PASSWORD_FILE"))

r = redis.Redis(host=REDIS_HOST, port=6379, password=REDIS_PASSWORD, decode_responses=False)

if not DHAN_CLIENT_ID or not DHAN_ACCESS_TOKEN:
    logging.warning("DHAN credentials not provided; strategy will run in simulate mode only.")

if not SIMULATE and (not DHAN_CLIENT_ID or not DHAN_ACCESS_TOKEN):
    logging.error("Cannot run live without DHAN credentials. Set SIMULATE=true to test without placing orders.")

if SECURITY_ID == "REPLACE_WITH_REAL_SECURITY_ID":
    logging.warning("SECURITY_ID not replaced; orders will not be placed until SECURITY_ID is configured.")
    if not SIMULATE:
        raise RuntimeError("SECURITY_ID must be configured for live mode")

# Strategy code should not initialize Dhan SDK. Order placement is handled
# by the `execution` module which may use `dhan_api` or the SDK as appropriate.


class BaseCandle:
    def __init__(self, start_ts):
        self.start = start_ts
        self.open = None
        self.high = -np.inf
        self.low = np.inf
        self.close = None

    def add_tick(self, price):
        if self.open is None:
            self.open = price
        self.high = max(self.high, price)
        self.low = min(self.low, price)
        self.close = price

    def to_dict(self):
        return {"time": self.start, "open": self.open, "high": self.high, "low": self.low, "close": self.close}


def compute_atr(df, period=14):
    if len(df) < period + 1:
        return None
    high = df["high"].values
    low = df["low"].values
    close = df["close"].values
    prev_close = np.roll(close, 1)
    tr = np.maximum.reduce([high - low, np.abs(high - prev_close), np.abs(low - prev_close)])
    tr[0] = high[0] - low[0]
    atr = pd.Series(tr).rolling(window=period).mean()
    return atr.iloc[-1]


def aggregate_candles(candles):
    # candles: list of dicts with open/high/low/close/time
    if not candles:
        return None
    opens = [c["open"] for c in candles]
    highs = [c["high"] for c in candles]
    lows = [c["low"] for c in candles]
    closes = [c["close"] for c in candles]
    start = candles[0]["time"]
    return {"time": start, "open": opens[0], "high": max(highs), "low": min(lows), "close": closes[-1]}


def place_order(side, quantity, price=None):
    """Deprecated helper — publish an ENTRY_SIGNAL instead of placing orders directly.

    Kept for backwards compatibility; prefer publishing `ENTRY_SIGNAL` from strategies.
    """
    logging.warning("strategy_service.place_order is deprecated; publishing ENTRY_SIGNAL instead")
    payload = {"symbol": SECURITY_ID, "side": side, "quantity": quantity, "price": price, "security_id": SECURITY_ID}
    publish("ENTRY_SIGNAL", payload)
    return {"status": "queued"}


def main():
    pubsub = r.pubsub()
    pubsub.subscribe("ltp:NIFTY")

    base_tf = "1m"
    higher_tf = "15m"

    base_candle = None
    base_candles = []  # finalized 1m candles

    last_min = None

    logging.info("Strategy started (SIMULATE=%s). Listening for ticks...", SIMULATE)

    while True:
        try:
            pubsub = r.pubsub()
            pubsub.subscribe("ltp:NIFTY")
            for msg in pubsub.listen():
                try:
                    if msg["type"] != "message":
                        continue
                    payload = json.loads(msg["data"])
                    ltp = float(payload.get("ltp") or 0)
                    ts = payload.get("ts")
                    if ts:
                        tick_time = datetime.fromtimestamp(float(ts))
                    else:
                        tick_time = datetime.utcnow()

                    minute = tick_time.replace(second=0, microsecond=0)

                    if last_min is None:
                        last_min = minute

                    if base_candle is None:
                        base_candle = BaseCandle(minute)

                    if minute != base_candle.start:
                        # finalize previous base candle
                        base_candles.append(base_candle.to_dict())
                        # keep only recent history
                        if len(base_candles) > 500:
                            base_candles = base_candles[-500:]
                        # start new candle
                        base_candle = BaseCandle(minute)

                        # check if we can build higher timeframe (15m)
                        if len(base_candles) >= 16:
                            # build 15m from last 15 completed 1m candles
                            recent_15 = base_candles[-15:]
                            h15 = aggregate_candles(recent_15)
                            # prepare DataFrame for ATR using aggregated 15m candles
                            # gather enough 1m candles to form (ATR_PERIOD + 20) 15m bars
                            needed_1m = 15 * (ATR_PERIOD + 20)
                            recent_1m = base_candles[-needed_1m:] if len(base_candles) >= needed_1m else base_candles[:]
                            agg15 = []
                            # group into 15m buckets
                            for i in range(0, len(recent_1m), 15):
                                block = recent_1m[i:i+15]
                                if len(block) == 15:
                                    agg15.append(aggregate_candles(block))
                            if agg15:
                                df15 = pd.DataFrame([{"high": c["high"], "low": c["low"], "close": c["close"]} for c in agg15])
                                atr = compute_atr(df15, period=ATR_PERIOD)
                            else:
                                atr = None
                            if atr is not None:
                                prev_high = max([c["high"] for c in recent_15[:-1]])
                                prev_low = min([c["low"] for c in recent_15[:-1]])
                                breakout_long = h15["close"] > (prev_high + ATR_MULTIPLIER * atr)
                                breakout_short = h15["close"] < (prev_low - ATR_MULTIPLIER * atr)
                                logging.info("15m ATR=%.4f prev_high=%.2f prev_low=%.2f close=%.2f", atr, prev_high, prev_low, h15["close"])

                                # Exit logic: if a position exists and opposite breakout occurs, request exit
                                if default_manager.has_open_position():
                                    positions = default_manager.list_positions()
                                    if positions:
                                        # take first active position
                                        pos_id = list(positions.keys())[0]
                                        pos = list(positions.values())[0]
                                        if pos.get('side') == 'BUY' and breakout_short:
                                            logging.info('Publishing EXIT_SIGNAL for %s due to short breakout', pos_id)
                                            publish('EXIT_SIGNAL', {'pos_id': pos_id, 'price': ltp})
                                        elif pos.get('side') == 'SELL' and breakout_long:
                                            logging.info('Publishing EXIT_SIGNAL for %s due to long breakout', pos_id)
                                            publish('EXIT_SIGNAL', {'pos_id': pos_id, 'price': ltp})

                                # Entry logic
                                if breakout_long or breakout_short:
                                    side = 'BUY' if breakout_long else 'SELL'

                                    # cooldown check
                                    now = datetime.utcnow().replace(tzinfo=timezone.utc)
                                    global last_trade_time_utc
                                    if last_trade_time_utc and (now - last_trade_time_utc).total_seconds() < MIN_TRADE_GAP:
                                        logging.info('Trade cooldown active; skipping entry')
                                    else:
                                        # ensure no existing open position (single-position protection)
                                        if default_manager.has_open_position():
                                            logging.warning('Position already open; skipping new entry')
                                        else:
                                            # determine stop loss from recent 15m bars
                                            stop_loss = None
                                            if breakout_long:
                                                stop_loss = prev_low
                                            else:
                                                stop_loss = prev_high

                                            # risk check and potential sizing
                                            try:
                                                approved, sized_qty = check_risk(side, LOT_SIZE, {'confidence_score': None}) if check_risk is not None else (True, LOT_SIZE)
                                            except Exception:
                                                approved, sized_qty = (False, LOT_SIZE)

                                            if approved:
                                                qty_to_use = int(sized_qty or LOT_SIZE)
                                                payload = {"symbol": SECURITY_ID, "side": side, "quantity": qty_to_use, "price": None, "security_id": SECURITY_ID, 'stop_loss': stop_loss}
                                                publish("ENTRY_SIGNAL", payload)
                                                logging.info("Published ENTRY_SIGNAL %s", {k: payload.get(k) for k in ('symbol','side','quantity','stop_loss')})
                                                last_trade_time_utc = datetime.utcnow().replace(tzinfo=timezone.utc)

                    # add tick to current base candle
                    base_candle.add_tick(ltp)
                except Exception:
                    logging.exception("Error while processing message")

        except Exception:
            logging.exception("Error while processing tick")


if __name__ == "__main__":
    main()
