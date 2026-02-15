import os
import json
import time
import logging
from datetime import datetime, timedelta

import redis
import pandas as pd
import numpy as np
from execution import place_order as exec_place_order
from risk import check_risk

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
    """Delegate order placement to the `execution` module."""
    logging.info("Strategy.place_order delegate: %s %s @%s", side, quantity, price)
    return exec_place_order(side, SECURITY_ID, quantity, price=price)


def main():
    pubsub = r.pubsub()
    pubsub.subscribe("ltp:NIFTY")

    base_tf = "1m"
    higher_tf = "15m"

    base_candle = None
    base_candles = []  # finalized 1m candles

    last_min = None

    logging.info("Strategy started (SIMULATE=%s). Listening for ticks...", SIMULATE)

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
                    # prepare DataFrame for ATR
                    df15 = pd.DataFrame([{
                        "high": c["high"],
                        "low": c["low"],
                        "close": c["close"]
                    } for c in base_candles[-(ATR_PERIOD + 20):]])
                    atr = compute_atr(df15, period=ATR_PERIOD)
                    if atr is not None:
                        prev_high = max([c["high"] for c in recent_15[:-1]])
                        prev_low = min([c["low"] for c in recent_15[:-1]])
                        breakout_long = h15["close"] > (prev_high + ATR_MULTIPLIER * atr)
                        breakout_short = h15["close"] < (prev_low - ATR_MULTIPLIER * atr)
                        logging.info("15m ATR=%.4f prev_high=%.2f prev_low=%.2f close=%.2f", atr, prev_high, prev_low, h15["close"])
                        if breakout_long:
                            logging.info("Breakout LONG detected")
                            if check_risk(0, "BUY", LOT_SIZE):
                                exec_place_order("BUY", SECURITY_ID, LOT_SIZE)
                        elif breakout_short:
                            logging.info("Breakout SHORT detected")
                            if check_risk(0, "SELL", LOT_SIZE):
                                exec_place_order("SELL", SECURITY_ID, LOT_SIZE)

            # add tick to current base candle
            base_candle.add_tick(ltp)

        except Exception:
            logging.exception("Error while processing tick")


if __name__ == "__main__":
    main()
