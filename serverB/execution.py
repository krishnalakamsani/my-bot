import os
import logging
from datetime import datetime

import psycopg2
import json
import threading
import hashlib
from config import bot_state, config
try:
    from dhan_api import DhanAPI
except Exception:
    DhanAPI = None

from event_bus import subscribe, publish
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

# Global re-entrant lock to serialize execution actions (entry/exit)
_exec_lock = threading.RLock()

# In-memory pending orders map: pos_id -> metadata
_pending_orders = {}


def _start_pending_monitor():
    def _monitor():
        while True:
            try:
                import time
                timeout = int(config.get('order_timeout_seconds', 30) or 30)
                now = datetime.utcnow()
                to_check = list(_pending_orders.items())
                for key, meta in to_check:
                    try:
                        placed = meta.get('placed_ts')
                        if not placed:
                            continue
                        age = (now - placed).total_seconds()
                        if age >= timeout:
                            # attempt best-effort cancel for live orders
                            try:
                                broker_info = meta.get('broker_info') or {}
                                if not SIMULATE:
                                    # attempt broker cancel if available
                                    try:
                                        if 'order_id' in broker_info and (DhanAPI is not None or True):
                                            # best-effort: use DhanAPI cancel if available
                                            try:
                                                if DhanAPI is not None:
                                                    client = DhanAPI(DHAN_ACCESS_TOKEN, DHAN_CLIENT_ID)
                                                    client.dhan.cancel_order(broker_info.get('order_id'))
                                                else:
                                                    from dhanhq import DhanContext
                                                    client = DhanContext(client_id=DHAN_CLIENT_ID, access_token=DHAN_ACCESS_TOKEN)
                                                    client.cancel_order(broker_info.get('order_id'))
                                            except Exception:
                                                pass
                            except Exception:
                                pass
                            # publish ORDER_TIMEOUT
                            try:
                                publish('ORDER_TIMEOUT', {'pos_id': key, 'db_id': meta.get('db_id'), 'info': meta, 'age_seconds': age})
                            except Exception:
                                pass
                            # mark DB record as timed-out
                            try:
                                cur = conn.cursor()
                                cur.execute("UPDATE trades SET status=%s WHERE id=%s", ("timed_out", meta.get('db_id')))
                                conn.commit()
                                cur.close()
                            except Exception:
                                pass
                            # remove from pending map
                            try:
                                _pending_orders.pop(key, None)
                            except Exception:
                                pass
                time.sleep(max(1, min(5, timeout//3 if timeout>0 else 5)))
            except Exception:
                try:
                    time.sleep(5)
                except Exception:
                    pass

    t = threading.Thread(target=_monitor, daemon=True, name='pending-order-monitor')
    t.start()


# start monitor on module load
try:
    _start_pending_monitor()
except Exception:
    pass


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
        "INSERT INTO trades (ts, side, quantity, price, status, info) VALUES (%s, %s, %s, %s, %s, %s) RETURNING id",
        (datetime.utcnow(), side, quantity, price, status, json.dumps(info) if info else None),
    )
    tid = cur.fetchone()[0]
    conn.commit()
    cur.close()
    return tid


def _compute_lock_key(key_str: str) -> int:
    try:
        if not key_str:
            return 0
        h = hashlib.md5(str(key_str).encode('utf-8')).hexdigest()
        return int(h, 16) % (2 ** 63 - 1)
    except Exception:
        return 0


def _acquire_advisory_lock(lock_key: int) -> bool:
    try:
        ensure_db()
        cur = conn.cursor()
        cur.execute("SELECT pg_try_advisory_lock(%s)", (lock_key,))
        res = cur.fetchone()[0]
        cur.close()
        return bool(res)
    except Exception:
        try:
            cur.close()
        except Exception:
            pass
        return False


def _release_advisory_lock(lock_key: int) -> None:
    try:
        ensure_db()
        cur = conn.cursor()
        cur.execute("SELECT pg_advisory_unlock(%s)", (lock_key,))
        cur.close()
    except Exception:
        try:
            cur.close()
        except Exception:
            pass


def _handle_entry_signal(payload):
    """Handle published ENTRY_SIGNAL events.

    Payload is expected to be a dict with keys: pos_id, symbol, side, quantity, price, security_id (opt)
    """
    lock_key = None
    got_lock = False
    try:
        side = payload.get('side')
        pos_id = payload.get('pos_id') or payload.get('id') or f"pos_{int(datetime.utcnow().timestamp())}"
        lock_key = _compute_lock_key(pos_id)
        # Try to acquire cross-process advisory lock first
        if lock_key:
            try:
                got_lock = _acquire_advisory_lock(lock_key)
            except Exception:
                got_lock = False
        if not got_lock:
            logging.warning("Could not acquire advisory lock for pos=%s; skipping entry to avoid duplicate placement", pos_id)
            return

        with _exec_lock:
            symbol = payload.get('symbol')
            quantity = int(payload.get('quantity') or 0)
            price = float(payload.get('price') or 0.0)
            security_id = payload.get('security_id')

            logging.info("Execution received ENTRY_SIGNAL %s %s %s @%s", side, symbol, quantity, price)

            # record in DB or simulate
            if SIMULATE:
                tid = record_trade(side, quantity, price or 0, status="simulated", info={"security_id": security_id, "pos_id": pos_id})
                # publish ORDER_PLACED then ORDER_FILLED immediately for simulated fills
                placed_payload = {'trade_id': pos_id, 'db_id': tid, 'pos_id': pos_id, 'security_id': security_id, 'symbol': symbol, 'qty': quantity, 'price': price, 'status': 'simulated', 'placed_ts': datetime.utcnow().isoformat()}
                publish('ORDER_PLACED', placed_payload)
                # track pending
                try:
                    _pending_orders[pos_id] = {'db_id': tid, 'pos_id': pos_id, 'placed_ts': datetime.utcnow(), 'qty': quantity, 'side': side, 'price': price, 'broker_info': None}
                except Exception:
                    pass
                default_manager.open_position(pos_id, symbol, side, quantity, price, security_id=security_id)
                publish('ORDER_FILLED', {'trade_id': pos_id, 'db_id': tid, 'pos_id': pos_id, 'security_id': security_id, 'symbol': symbol, 'filled_qty': quantity, 'filled_price': price, 'status': 'simulated', 'filled_ts': datetime.utcnow().isoformat()})
                return

            if not DHAN_CLIENT_ID or not DHAN_ACCESS_TOKEN:
                logging.error("Dhan credentials missing; cannot place live order")
                return

            # Live order placement (isolated try so we can record failures)
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

                tid = record_trade(side, quantity, price or 0, status="sent", info={'res': res, 'pos_id': pos_id})
                # publish ORDER_PLACED
                try:
                    placed_payload = {'trade_id': pos_id, 'db_id': tid, 'pos_id': pos_id, 'security_id': security_id, 'symbol': symbol, 'qty': quantity, 'price': price, 'status': 'sent', 'broker_info': res, 'placed_ts': datetime.utcnow().isoformat()}
                    publish('ORDER_PLACED', placed_payload)
                except Exception:
                    logging.exception('Failed to publish ORDER_PLACED')

                # register the position best-effort
                default_manager.open_position(pos_id, symbol, side, quantity, price, security_id=security_id)
                # track pending order for timeout/slippage monitoring
                try:
                    _pending_orders[pos_id] = {'db_id': tid, 'pos_id': pos_id, 'placed_ts': datetime.utcnow(), 'qty': quantity, 'side': side, 'price': price, 'broker_info': res}
                except Exception:
                    pass

                # best-effort: if broker reports an immediate fill, emit ORDER_FILLED
                try:
                    filled = False
                    filled_qty = None
                    filled_price = None
                    if isinstance(res, dict):
                        if res.get('status') and str(res.get('status')).lower() in ('filled', 'complete', 'filled_with_trade'):
                            filled = True
                        filled_qty = res.get('filled_quantity') or res.get('filledQty') or res.get('filled_qty')
                        filled_price = res.get('avg_price') or res.get('filled_price') or res.get('avgPrice')
                    if filled or (filled_qty and filled_price):
                        publish('ORDER_FILLED', {'trade_id': pos_id, 'db_id': tid, 'pos_id': pos_id, 'security_id': security_id, 'symbol': symbol, 'filled_qty': filled_qty or quantity, 'filled_price': filled_price or price, 'status': 'filled', 'broker_info': res, 'filled_ts': datetime.utcnow().isoformat()})
                except Exception:
                    logging.exception('Failed to evaluate broker fill and publish ORDER_FILLED')
            except Exception:
                logging.exception('Live order failed')
                record_trade(side, quantity, price or 0, status="failed")
    except Exception:
        logging.exception("Execution ENTRY handler failed")
    finally:
        try:
            if got_lock and lock_key:
                _release_advisory_lock(lock_key)
        except Exception:
            pass


def _handle_exit_signal(payload):
    """Handle published EXIT_SIGNAL events. Expect pos_id or security_id and price."""
    lock_key = None
    got_lock = False
    try:
        pos_id = payload.get('pos_id')
        price = float(payload.get('price') or 0.0)
        security_id = payload.get('security_id')

        logging.info("Execution received EXIT_SIGNAL pos=%s sec=%s @%s", pos_id, security_id, price)

        if pos_id:
            lock_key = _compute_lock_key(pos_id)
            if lock_key:
                try:
                    got_lock = _acquire_advisory_lock(lock_key)
                except Exception:
                    got_lock = False
            if not got_lock:
                logging.warning("Could not acquire advisory lock for pos=%s; skipping exit to avoid duplicate close", pos_id)
                return

            with _exec_lock:
                p = default_manager.get_position(pos_id)
                if not p:
                    logging.warning("Unknown position %s for exit", pos_id)
                    return
                default_manager.close_position(pos_id, price)
                record_trade(p.side, p.quantity, price or 0, status="closed", info={"pos_id": pos_id})
                return

        # fallback: try to close by security_id
        # iterate positions and close first match; lock per-position
        for pid, pd in default_manager.list_positions().items():
            try:
                if str(pd.get('security_id') or '') == str(security_id):
                    lk = _compute_lock_key(pid)
                    locked = False
                    try:
                        if lk:
                            locked = _acquire_advisory_lock(lk)
                    except Exception:
                        locked = False
                    if not locked:
                        logging.warning("Could not acquire advisory lock for pos=%s; skipping this candidate", pid)
                        continue
                    try:
                        with _exec_lock:
                            default_manager.close_position(pid, price)
                            record_trade(pd.get('side'), pd.get('quantity'), price or 0, status="closed", info={"pos_id": pid})
                            return
                    finally:
                        try:
                            if lk:
                                _release_advisory_lock(lk)
                        except Exception:
                            pass
            except Exception:
                logging.exception('Error iterating positions for exit')
    except Exception:
        logging.exception("Error handling exit signal")
    finally:
        try:
            if got_lock and lock_key:
                _release_advisory_lock(lock_key)
        except Exception:
            pass


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

