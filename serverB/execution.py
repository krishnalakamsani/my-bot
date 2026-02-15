import os
import logging
from datetime import datetime

import psycopg2
import json
try:
    from dhan_api import DhanAPI
except Exception:
    DhanAPI = None

from event_bus import subscribe
from position_manager import default_manager

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


def _handle_entry_signal(payload):
    """Handle published ENTRY_SIGNAL events.

    Payload is expected to be a dict with keys: pos_id, symbol, side, quantity, price, security_id (opt)
    """
    try:
        side = payload.get('side')
        pos_id = payload.get('pos_id') or payload.get('id') or f"pos_{int(datetime.utcnow().timestamp())}"
        symbol = payload.get('symbol')
        quantity = int(payload.get('quantity') or 0)
        price = float(payload.get('price') or 0.0)
        security_id = payload.get('security_id')

        logging.info("Execution received ENTRY_SIGNAL %s %s %s @%s", side, symbol, quantity, price)

        # record in DB or simulate
        if SIMULATE:
            record_trade(side, quantity, price or 0, status="simulated", info={"security_id": security_id})
            default_manager.open_position(pos_id, symbol, side, quantity, price, security_id=security_id)
            return

        if not DHAN_CLIENT_ID or not DHAN_ACCESS_TOKEN:
            logging.error("Dhan credentials missing; cannot place live order")
            return

        # Prefer the DhanAPI wrapper if available
        try:
            if DhanAPI is not None:
                client = DhanAPI(DHAN_ACCESS_TOKEN, DHAN_CLIENT_ID)
                res = client.dhan.place_order(
                    security_id=security_id,
                    exch_seg="NSE",
                    transaction_type=side,
                    quantity=quantity,
                    order_type="MARKET",
                    price=price,
                    product_type="INTRADAY",
                )
            else:
                from dhanhq import DhanContext

                client = DhanContext(client_id=DHAN_CLIENT_ID, access_token=DHAN_ACCESS_TOKEN)
                res = client.place_order(
                    security_id=security_id,
                    exch_seg="NSE",
                    transaction_type=side,
                    quantity=quantity,
                    order_type="MARKET",
                    price=price,
                    product_type="INTRADAY",
                )

            record_trade(side, quantity, price or 0, status="sent", info=res)
            # on success, register position (best-effort; order fills may differ)
            default_manager.open_position(pos_id, symbol, side, quantity, price, security_id=security_id)
        except Exception:
            logging.exception("Live order failed")
            record_trade(side, quantity, price or 0, status="failed")


def _handle_exit_signal(payload):
    """Handle published EXIT_SIGNAL events. Expect pos_id or security_id and price."""
    try:
        pos_id = payload.get('pos_id')
        price = float(payload.get('price') or 0.0)
        security_id = payload.get('security_id')

        logging.info("Execution received EXIT_SIGNAL pos=%s sec=%s @%s", pos_id, security_id, price)

        if pos_id:
            p = default_manager.get_position(pos_id)
            if not p:
                logging.warning("Unknown position %s for exit", pos_id)
                return
            default_manager.close_position(pos_id, price)
            record_trade(p.side, p.quantity, price or 0, status="closed", info={"pos_id": pos_id})
            return

        # fallback: try to close by security_id
        # iterate positions and close first match
        for pid, pd in default_manager.list_positions().items():
            if str(pd.get('security_id') or '') == str(security_id):
                default_manager.close_position(pid, price)
                record_trade(pd.get('side'), pd.get('quantity'), price or 0, status="closed", info={"pos_id": pid})
                return
    except Exception:
        logging.exception("Error handling exit signal")


# Register handlers on import so the event-driven pattern works without extra wiring.
try:
    subscribe("ENTRY_SIGNAL", _handle_entry_signal)
    subscribe("EXIT_SIGNAL", _handle_exit_signal)
except Exception:
    logging.exception("Failed to subscribe execution handlers")


def place_order(side, security_id, quantity, order_type="MARKET", price=None):
    """Backward-compatible wrapper: publishes ENTRY_SIGNAL for callers that still call place_order directly."""
    payload = {"side": side, "symbol": security_id, "quantity": quantity, "price": price, "security_id": security_id}
    # import inside function to avoid circular import at module load
    from event_bus import publish
    publish("ENTRY_SIGNAL", payload)
    return {"status": "queued"}

