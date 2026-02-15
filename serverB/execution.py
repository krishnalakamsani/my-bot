import os
import logging
from datetime import datetime
from datetime import time as _time, timedelta

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
try:
    from risk import check_risk
except Exception:
    check_risk = None

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


_exec_lock = threading.RLock()

# In-memory pending orders map: pos_id -> metadata
_pending_orders = {}


def get_conn():
    return psycopg2.connect(host=POSTGRES_HOST, dbname=POSTGRES_DB, user=POSTGRES_USER, password=POSTGRES_PASSWORD)


def _start_pending_monitor():
    def _monitor():
        import time
        while True:
            try:
                timeout = int(config.get('order_timeout_seconds', 30) or 30)
                now = datetime.utcnow()
                for key, meta in list(_pending_orders.items()):
                    placed = meta.get('placed_ts')
                    if not placed:
                        continue
                    try:
                        age = (now - placed).total_seconds()
                    except Exception:
                        continue
                    if age < timeout:
                        continue

                    broker_info = meta.get('broker_info') or {}
                    # attempt best-effort cancel for live orders
                    if (not SIMULATE) and broker_info and broker_info.get('order_id'):
                        try:
                            if DhanAPI is not None:
                                client = DhanAPI(DHAN_ACCESS_TOKEN, DHAN_CLIENT_ID)
                                client.dhan.cancel_order(broker_info.get('order_id'))
                            else:
                                from dhanhq import DhanContext
                                client = DhanContext(client_id=DHAN_CLIENT_ID, access_token=DHAN_ACCESS_TOKEN)
                                client.cancel_order(broker_info.get('order_id'))
                        except Exception:
                            logging.exception('Broker cancel failed')

                    # publish ORDER_TIMEOUT
                    try:
                        publish('ORDER_TIMEOUT', {'pos_id': key, 'db_id': meta.get('db_id'), 'info': meta, 'age_seconds': age})
                    except Exception:
                        logging.exception('Failed to publish ORDER_TIMEOUT')

                    # mark DB record as timed-out
                    try:
                        with get_conn() as _c:
                            with _c.cursor() as cur:
                                cur.execute("UPDATE trades SET status=%s WHERE id=%s", ("timed_out", meta.get('db_id')))
                    except Exception:
                        logging.exception('Failed to mark DB timed_out')

                    # remove from pending map
                    _pending_orders.pop(key, None)

                time.sleep(max(1, min(5, timeout // 3 if timeout > 0 else 5)))
            except Exception:
                logging.exception('pending monitor loop error')
                time.sleep(5)

    t = threading.Thread(target=_monitor, daemon=True, name='pending-order-monitor')
    t.start()


# start monitor on module load
try:
    _start_pending_monitor()
except Exception:
    pass


def ensure_db():
    # Ensure trades table exists
    try:
        with get_conn() as _c:
            with _c.cursor() as cur:
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
    except Exception:
        logging.exception('Failed to ensure trades table')


def is_market_open() -> bool:
    """Return True if NSE market is open right now (approximate, based on IST timezone).

    - Uses UTC time +5:30 to derive IST.
    - Considers weekdays Monday-Friday and market hours 09:15-15:30 IST.
    This is intentionally conservative and simple; integrate broker market-status API if available.
    """
    try:
        now_utc = datetime.utcnow()
        ist = now_utc + timedelta(hours=5, minutes=30)
        # weekday: Monday=0 .. Friday=4
        if ist.weekday() >= 5:
            return False
        start = _time(hour=9, minute=15)
        end = _time(hour=15, minute=30)
        return start <= ist.time() <= end
    except Exception:
        return False


def record_trade(side, quantity, price, status="created", info=None):
    ensure_db()
    try:
        with get_conn() as _c:
            with _c.cursor() as cur:
                cur.execute(
                    "INSERT INTO trades (ts, side, quantity, price, status, info) VALUES (%s, %s, %s, %s, %s, %s) RETURNING id",
                    (datetime.utcnow(), side, quantity, price, status, json.dumps(info) if info else None),
                )
                tid = cur.fetchone()[0]
            return tid
    except Exception:
        logging.exception('Failed to record trade')
        return None


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
        with get_conn() as _c:
            with _c.cursor() as cur:
                cur.execute("SELECT pg_try_advisory_lock(%s)", (lock_key,))
                res = cur.fetchone()[0]
                return bool(res)
    except Exception:
        logging.exception('Failed to acquire advisory lock')
        return False


def _release_advisory_lock(lock_key: int) -> None:
    try:
        with get_conn() as _c:
            with _c.cursor() as cur:
                cur.execute("SELECT pg_advisory_unlock(%s)", (lock_key,))
    except Exception:
        logging.exception('Failed to release advisory lock')


def _handle_entry_signal(payload):
    """Handle published ENTRY_SIGNAL events.

    Payload keys: pos_id, symbol, side, quantity, price, security_id (opt)
    """
    side = payload.get('side')
    pos_id = payload.get('pos_id') or payload.get('id') or f"pos_{int(datetime.utcnow().timestamp())}"
    lock_key = _compute_lock_key(pos_id)
    got_lock = False

    # Try to acquire cross-process advisory lock
    if lock_key:
        got_lock = _acquire_advisory_lock(lock_key)
        if not got_lock:
            logging.warning("Could not acquire advisory lock for pos=%s; skipping entry", pos_id)
            return

    try:
        # centralized risk check if available
        try:
            if check_risk is not None:
                requested_q = int(payload.get('quantity') or 0)
                approved, sized_q = check_risk(side, requested_q, payload)
                if not approved:
                    logging.warning('Risk check rejected entry for pos=%s', pos_id)
                    return
                # allow risk module to size the order
                if sized_q and int(sized_q) > 0:
                    payload['quantity'] = int(sized_q)
        except Exception:
            logging.exception('Risk module error; falling back to local checks')
            # fallback local daily max loss guard
            if bool(bot_state.get('daily_max_loss_triggered', False)):
                logging.warning('Daily max loss triggered; blocking entry for pos=%s', pos_id)
                return
            daily_pnl = float(bot_state.get('daily_pnl', 0.0) or 0.0)
            daily_max_loss = float(config.get('daily_max_loss', 0) or 0)
            if daily_max_loss and daily_pnl <= -abs(daily_max_loss):
                logging.warning('Daily max loss exceeded; blocking entry for pos=%s', pos_id)
                bot_state['daily_max_loss_triggered'] = True
                bot_state['trading_enabled'] = False
                return

        with _exec_lock:
            symbol = payload.get('symbol')
            quantity = int(payload.get('quantity') or 0)
            # if risk module already sized the order, use it
            try:
                if check_risk is not None and payload.get('quantity') is not None:
                    quantity = int(payload.get('quantity'))
            except Exception:
                pass
            if quantity <= 0:
                logging.error("Invalid quantity for entry: %s", quantity)
                return
            price = float(payload.get('price') or 0.0)
            security_id = payload.get('security_id')

            logging.info("Execution received ENTRY_SIGNAL %s %s %s @%s", side, symbol, quantity, price)

            if SIMULATE:
                tid = record_trade(side, quantity, price or 0, status="simulated", info={"security_id": security_id, "pos_id": pos_id})
                placed_payload = {'trade_id': pos_id, 'db_id': tid, 'pos_id': pos_id, 'security_id': security_id, 'symbol': symbol, 'qty': quantity, 'price': price, 'status': 'simulated', 'placed_ts': datetime.utcnow().isoformat()}
                publish('ORDER_PLACED', placed_payload)
                _pending_orders[pos_id] = {'db_id': tid, 'pos_id': pos_id, 'placed_ts': datetime.utcnow(), 'qty': quantity, 'side': side, 'price': price, 'broker_info': None, 'simulated': True}

                if not is_market_open():
                    default_manager.open_position(pos_id, symbol, side, quantity, price, security_id=security_id)
                    publish('ORDER_FILLED', {'trade_id': pos_id, 'db_id': tid, 'pos_id': pos_id, 'security_id': security_id, 'symbol': symbol, 'filled_qty': quantity, 'filled_price': price, 'status': 'simulated', 'filled_ts': datetime.utcnow().isoformat()})
                    _pending_orders.pop(pos_id, None)
                else:
                    logging.warning('Market open; not simulating fill for pos=%s; left pending', pos_id)
                return

            # Live order
            if not DHAN_CLIENT_ID or not DHAN_ACCESS_TOKEN:
                logging.error("Dhan credentials missing; cannot place live order")
                return

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

                if isinstance(res, dict) and str(res.get('status', '')).lower() in ("rejected", "failed"):
                    logging.error("Broker rejected entry order: %s", res)
                    record_trade(side, quantity, price or 0, status="rejected", info={'res': res, 'pos_id': pos_id})
                    return

                tid = record_trade(side, quantity, price or 0, status="sent", info={'res': res, 'pos_id': pos_id})
                publish('ORDER_PLACED', {'trade_id': pos_id, 'db_id': tid, 'pos_id': pos_id, 'security_id': security_id, 'symbol': symbol, 'qty': quantity, 'price': price, 'status': 'sent', 'broker_info': res, 'placed_ts': datetime.utcnow().isoformat()})
                default_manager.open_position(pos_id, symbol, side, quantity, price, security_id=security_id)
                _pending_orders[pos_id] = {'db_id': tid, 'pos_id': pos_id, 'placed_ts': datetime.utcnow(), 'qty': quantity, 'side': side, 'price': price, 'broker_info': res}

                # immediate fill detection
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
                    _pending_orders.pop(pos_id, None)

                    # place broker-side SL
                    try:
                        sl_points = float(config.get('initial_stoploss', 0) or 0)
                        if sl_points and not SIMULATE:
                            entry_price = float(filled_price or price or 0)
                            if entry_price:
                                if str(side).upper() == 'BUY':
                                    trigger = max(0.0, entry_price - sl_points)
                                    sl_side = 'SELL'
                                else:
                                    trigger = entry_price + sl_points
                                    sl_side = 'BUY'
                                if DhanAPI is not None:
                                    client = DhanAPI(DHAN_ACCESS_TOKEN, DHAN_CLIENT_ID)
                                    client.dhan.place_order(security_id=security_id, exch_seg='NSE', transaction_type=sl_side, quantity=quantity, order_type='SL-M', trigger_price=trigger, product_type='INTRADAY')
                                else:
                                    from dhanhq import DhanContext
                                    client = DhanContext(client_id=DHAN_CLIENT_ID, access_token=DHAN_ACCESS_TOKEN)
                                    client.place_order(security_id=security_id, exch_seg='NSE', transaction_type=sl_side, quantity=quantity, order_type='SL-M', trigger_price=trigger, product_type='INTRADAY')
                    except Exception:
                        logging.exception('Failed placing broker SL order')

            except Exception:
                logging.exception('Live order failed')
                record_trade(side, quantity, price or 0, status="failed")

    finally:
        if got_lock and lock_key:
            try:
                _release_advisory_lock(lock_key)
            except Exception:
                logging.exception('Failed to release advisory lock')


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
                # validate quantity
                try:
                    qty = int(p.get('quantity') or p.get('qty') or 0)
                except Exception:
                    qty = 0
                if qty <= 0:
                    logging.error("Invalid quantity for exit for pos %s: %s", pos_id, qty)
                    return

                # For live mode: place reverse order at broker first
                if not SIMULATE:
                    if not DHAN_CLIENT_ID or not DHAN_ACCESS_TOKEN:
                        logging.error("Dhan credentials missing; cannot place live exit order")
                        return
                    try:
                        if DhanAPI is not None:
                            client = DhanAPI(DHAN_ACCESS_TOKEN, DHAN_CLIENT_ID)
                            res = client.dhan.place_order(
                                security_id=p.get('security_id'),
                                exch_seg="NSE",
                                transaction_type=("SELL" if str(p.get('side')).upper() == 'BUY' else "BUY"),
                                quantity=qty,
                                order_type="MARKET",
                                product_type="INTRADAY",
                            )
                        else:
                            from dhanhq import DhanContext

                            client = DhanContext(client_id=DHAN_CLIENT_ID, access_token=DHAN_ACCESS_TOKEN)
                            res = client.place_order(
                                security_id=p.get('security_id'),
                                exch_seg="NSE",
                                transaction_type=("SELL" if str(p.get('side')).upper() == 'BUY' else "BUY"),
                                quantity=qty,
                                order_type="MARKET",
                                product_type="INTRADAY",
                            )
                    except Exception:
                        logging.exception("Live exit order failed")
                        return

                    # validate broker response
                    try:
                        if isinstance(res, dict) and str(res.get('status', '')).lower() in ("rejected", "failed"):
                            logging.error("Broker rejected exit order: %s", res)
                            record_trade(p.get('side'), qty, price or 0, status="rejected", info={'res': res, 'pos_id': pos_id})
                            return
                    except Exception:
                        pass

                    # record trade placement and track pending
                    try:
                        tid = record_trade(("SELL" if str(p.get('side')).upper() == 'BUY' else "BUY"), qty, price or 0, status="sent", info={'res': res, 'pos_id': pos_id})
                    except Exception:
                        tid = None
                    try:
                        placed_payload = {'trade_id': pos_id, 'db_id': tid, 'pos_id': pos_id, 'security_id': p.get('security_id'), 'symbol': p.get('symbol'), 'qty': qty, 'price': price, 'status': 'sent', 'broker_info': res, 'placed_ts': datetime.utcnow().isoformat()}
                        publish('ORDER_PLACED', placed_payload)
                    except Exception:
                        logging.exception('Failed to publish ORDER_PLACED for exit')
                    try:
                        _pending_orders[pos_id] = {'db_id': tid, 'pos_id': pos_id, 'placed_ts': datetime.utcnow(), 'qty': qty, 'side': ("SELL" if str(p.get('side')).upper() == 'BUY' else "BUY"), 'price': price, 'broker_info': res}
                    except Exception:
                        pass

                    # best-effort immediate fill detection
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
                            # broker reports filled: finalize internal close
                            default_manager.close_position(pos_id, filled_price or price)
                            record_trade(p.get('side'), qty, filled_price or price or 0, status="closed", info={"pos_id": pos_id, 'broker_info': res})
                            # cleanup pending
                            try:
                                _pending_orders.pop(pos_id, None)
                            except Exception:
                                pass
                            return
                    except Exception:
                        logging.exception('Failed to evaluate broker fill for exit')

                    # otherwise leave position open internally until ORDER_FILLED from broker or reconciliation
                    logging.info('Exit order sent to broker for pos=%s; awaiting fill', pos_id)
                    return

                # SIMULATE: never simulate exit fills while market is open
                if SIMULATE and is_market_open():
                    # publish ORDER_PLACED and keep pending; do not close internally
                    tid = record_trade(("SELL" if str(p.get('side')).upper() == 'BUY' else "BUY"), qty, price or 0, status="simulated", info={'pos_id': pos_id})
                    try:
                        placed_payload = {'trade_id': pos_id, 'db_id': tid, 'pos_id': pos_id, 'security_id': p.get('security_id'), 'symbol': p.get('symbol'), 'qty': qty, 'price': price, 'status': 'simulated', 'placed_ts': datetime.utcnow().isoformat()}
                        publish('ORDER_PLACED', placed_payload)
                    except Exception:
                        logging.exception('Failed to publish ORDER_PLACED for simulated exit')
                    try:
                        _pending_orders[pos_id] = {'db_id': tid, 'pos_id': pos_id, 'placed_ts': datetime.utcnow(), 'qty': qty, 'side': ("SELL" if str(p.get('side')).upper() == 'BUY' else "BUY"), 'price': price, 'broker_info': None, 'simulated': True}
                    except Exception:
                        pass
                    logging.warning('Market open; simulated exit for pos=%s will not auto-close position', pos_id)
                    return
                # SIMULATE and market closed: just close internally
                default_manager.close_position(pos_id, price)
                record_trade(p.get('side'), qty, price or 0, status="closed", info={"pos_id": pos_id})
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


def _handle_order_filled_cleanup(payload):
    try:
        if not payload:
            return
        pos_id = payload.get('pos_id') or payload.get('trade_id') or payload.get('db_id')
        if pos_id:
            try:
                _pending_orders.pop(pos_id, None)
            except Exception:
                pass
    except Exception:
        logging.exception('Error cleaning pending orders on ORDER_FILLED')


# Register handlers on import so the event-driven pattern works without extra wiring.
try:
    subscribe("ENTRY_SIGNAL", _handle_entry_signal)
    subscribe("EXIT_SIGNAL", _handle_exit_signal)
    subscribe("ORDER_FILLED", _handle_order_filled_cleanup)
except Exception:
    logging.exception("Failed to subscribe execution handlers")


def place_order(side, security_id, quantity, order_type="MARKET", price=None):
    """Backward-compatible wrapper: publishes ENTRY_SIGNAL for callers that still call place_order directly."""
    payload = {"side": side, "symbol": security_id, "quantity": quantity, "price": price, "security_id": security_id}
    # import inside function to avoid circular import at module load
    from event_bus import publish
    publish("ENTRY_SIGNAL", payload)
    return {"status": "queued"}
