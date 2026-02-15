import os
import json
import logging
from datetime import datetime

import redis
import psycopg2

logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(levelname)s %(message)s")

REDIS_HOST = os.getenv("REDIS_HOST", "redis")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_DB = os.getenv("POSTGRES_DB", "candles")
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")


def _read_secret_from_file(path):
    try:
        if path and os.path.exists(path):
            return open(path, "r").read().strip()
    except Exception:
        pass
    return None

REDIS_PASSWORD = os.getenv("REDIS_PASSWORD") or _read_secret_from_file(os.getenv("REDIS_PASSWORD_FILE"))

r = redis.Redis(host=REDIS_HOST, port=6379, password=REDIS_PASSWORD, decode_responses=False)

conn = None


def ensure_db():
    global conn
    if conn:
        return
    conn = psycopg2.connect(host=POSTGRES_HOST, dbname=POSTGRES_DB, user=POSTGRES_USER, password=POSTGRES_PASSWORD)
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS candles (
            symbol TEXT NOT NULL,
            ts TIMESTAMP NOT NULL,
            open DOUBLE PRECISION,
            high DOUBLE PRECISION,
            low DOUBLE PRECISION,
            close DOUBLE PRECISION,
            PRIMARY KEY (symbol, ts)
        )
        """
    )
    conn.commit()
    cur.close()


class Candle:
    def __init__(self, start):
        self.start = start
        self.open = None
        self.high = -9999999
        self.low = 9999999
        self.close = None

    def add(self, price):
        if self.open is None:
            self.open = price
        self.high = max(self.high, price)
        self.low = min(self.low, price)
        self.close = price


def persist_candle(symbol: str, c: Candle):
    ensure_db()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO candles (symbol, ts, open, high, low, close) VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT (symbol, ts) DO UPDATE SET open=EXCLUDED.open, high=EXCLUDED.high, low=EXCLUDED.low, close=EXCLUDED.close",
        (symbol, c.start, c.open, c.high, c.low, c.close),
    )
    conn.commit()
    cur.close()
    logging.info("Persisted candle %s %s O:%.2f H:%.2f L:%.2f C:%.2f", symbol, c.start, c.open, c.high, c.low, c.close)


def main():
    pubsub = r.pubsub()
    # Subscribe to all LTP channels; feed_service publishes `ltp:<INDEX>` and `ltp:SEC_<id>`
    pubsub.psubscribe("ltp:*")

    # Track open candles per symbol
    current_candles: dict[str, Candle] = {}

    for msg in pubsub.listen():
        try:
            if msg.get("type") not in ("message", "pmessage"):
                continue

            # channel can be bytes; decode if necessary
            channel = msg.get("channel") or msg.get("pattern")
            if isinstance(channel, bytes):
                channel = channel.decode('utf-8')

            # when pmessage, 'data' is message and 'pattern' present
            data_raw = msg.get("data")
            if isinstance(data_raw, (bytes, str)):
                try:
                    data = json.loads(data_raw)
                except Exception:
                    data = {}
            else:
                data = data_raw or {}

            # channel format: ltp:<SYMBOL> or ltp:SEC_<id>
            if isinstance(channel, str) and channel.startswith('ltp:'):
                symbol = channel.split(':', 1)[1]
            else:
                continue

            ltp = float((data.get('ltp') or 0) or 0)
            ts = data.get('ts')
            if ts:
                tick_time = datetime.fromtimestamp(float(ts))
            else:
                tick_time = datetime.utcnow()

            minute = tick_time.replace(second=0, microsecond=0)

            current = current_candles.get(symbol)
            if current is None:
                current = Candle(minute)
                current_candles[symbol] = current

            if minute != current.start:
                # persist previous
                try:
                    persist_candle(symbol, current)
                except Exception:
                    logging.exception("Error persisting candle")
                # start new
                current = Candle(minute)
                current_candles[symbol] = current

            current.add(ltp)

        except Exception:
            logging.exception("Error in candle builder loop")


if __name__ == "__main__":
    main()
