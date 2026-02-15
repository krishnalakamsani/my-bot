import os
import logging
from datetime import datetime

import psycopg2
import json
from dhanhq import DhanContext

logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(levelname)s %(message)s")

def _read_secret_from_file(path):
    try:
        if path and os.path.exists(path):
            return open(path, "r").read().strip()
    except Exception:
        pass
    return None

DHAN_CLIENT_ID = os.getenv("DHAN_CLIENT_ID") or _read_secret_from_file(os.getenv("DHAN_CLIENT_ID_FILE"))
DHAN_ACCESS_TOKEN = os.getenv("DHAN_ACCESS_TOKEN") or _read_secret_from_file(os.getenv("DHAN_ACCESS_TOKEN_FILE"))
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_DB = os.getenv("POSTGRES_DB", "trades")
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD") or _read_secret_from_file(os.getenv("POSTGRES_PASSWORD_FILE")) or "postgres"
SIMULATE = os.getenv("SIMULATE", "true").lower() in ("1", "true", "yes")

conn = None

def ensure_db():
    global conn
    if conn:
        return
    conn = psycopg2.connect(host=POSTGRES_HOST, dbname=POSTGRES_DB, user=POSTGRES_USER, password=POSTGRES_PASSWORD)
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS trades (
            id SERIAL PRIMARY KEY,
            ts TIMESTAMP,
            side TEXT,
            quantity INTEGER,
            price DOUBLE PRECISION,
            status TEXT,
            info JSONB
        )
        """
    )
    conn.commit()
    cur.close()


def record_trade(side, quantity, price, status="created", info=None):
    ensure_db()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO trades (ts, side, quantity, price, status, info) VALUES (%s, %s, %s, %s, %s, %s)",
        (datetime.utcnow(), side, quantity, price, status, json.dumps(info) if info else None),
    )
    conn.commit()
    cur.close()


def place_order(side, security_id, quantity, order_type="MARKET", price=None):
    logging.info("Execution.place_order: %s %s %s @%s", side, security_id, quantity, price)
    if SIMULATE:
        logging.info("SIMULATE=true — not placing real order")
        record_trade(side, quantity, price or 0, status="simulated", info={"security_id": security_id})
        return {"status": "simulated"}

    if not DHAN_CLIENT_ID or not DHAN_ACCESS_TOKEN:
        logging.error("Dhan credentials missing; cannot place live order")
        return None

    try:
        client = DhanContext(client_id=DHAN_CLIENT_ID, access_token=DHAN_ACCESS_TOKEN)
        res = client.place_order(
            security_id=security_id,
            exch_seg="NSE",
            transaction_type=side,
            quantity=quantity,
            order_type=order_type,
            price=price,
            product_type="INTRADAY",
        )
        record_trade(side, quantity, price or 0, status="sent", info=res)
        return res
    except Exception:
        logging.exception("Live order failed")
        record_trade(side, quantity, price or 0, status="failed")
        return None
