import os
import time
import json
import logging
import redis

try:
    from dhanhq import dhanhq
except Exception:
    dhanhq = None

logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(levelname)s %(message)s")

REDIS_HOST = os.getenv("REDIS_HOST", "redis")
DHAN_CLIENT_ID = os.getenv("DHAN_CLIENT_ID")
DHAN_ACCESS_TOKEN = os.getenv("DHAN_ACCESS_TOKEN")


def _read_secret_from_file(path):
    try:
        if path and os.path.exists(path):
            return open(path, "r").read().strip()
    except Exception:
        pass
    return None

REDIS_PASSWORD = os.getenv("REDIS_PASSWORD") or _read_secret_from_file(os.getenv("REDIS_PASSWORD_FILE"))

r = redis.Redis(host=REDIS_HOST, port=6379, password=REDIS_PASSWORD, decode_responses=False)

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_DB = os.getenv("POSTGRES_DB", "candles")
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD") or _read_secret_from_file(os.getenv("POSTGRES_PASSWORD_FILE"))

import psycopg2

pg_conn = None

def ensure_pg():
    global pg_conn
    if pg_conn:
        return
    try:
        pg_conn = psycopg2.connect(host=POSTGRES_HOST, dbname=POSTGRES_DB, user=POSTGRES_USER, password=POSTGRES_PASSWORD)
        cur = pg_conn.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS option_chains (
                idx TEXT NOT NULL,
                expiry TEXT NOT NULL,
                payload JSONB,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (idx, expiry)
            )
            """
        )
        pg_conn.commit()
        cur.close()
    except Exception:
        logging.exception("Failed to ensure Postgres connection/table for option_chains")

# Basic index definitions (kept in-sync with the backend repo indices)
INDICES = {
    "NIFTY": {"security_id": 13, "exchange_segment": "IDX_I", "fno_segment": "NSE_FNO"},
    "BANKNIFTY": {"security_id": 25, "exchange_segment": "IDX_I", "fno_segment": "NSE_FNO"},
    "SENSEX": {"security_id": 51, "exchange_segment": "IDX_I", "fno_segment": "BSE_FNO"},
    "FINNIFTY": {"security_id": 27, "exchange_segment": "IDX_I", "fno_segment": "NSE_FNO"},
}


def _extract_security_id(opt_payload: object) -> str:
    if not isinstance(opt_payload, dict):
        return ""
    security_id = opt_payload.get("security_id")
    if security_id is None:
        security_id = opt_payload.get("securityId")
    if security_id is None and isinstance(opt_payload.get("instrument"), dict):
        security_id = opt_payload["instrument"].get("security_id")
    return str(security_id) if security_id else ""


def publish_index_and_options(client):
    """Fetch index LTPs and option chains (oc) and publish to Redis.

    - Index LTPs published to `ltp:<INDEX>` (JSON: {ltp, ts})
    - Option LTPs published to `ltp:SEC_<SECID>` (JSON: {ltp, ts})
    - Option chain payload published to `oc:<INDEX>:<EXPIRY>` (raw JSON payload)
    """
    now_ts = int(time.time())
    for idx, cfg in INDICES.items():
        try:
            seg = cfg.get("exchange_segment")
            sec = cfg.get("security_id")

            # Fetch index quote
            resp = client.quote_data({seg: [sec]})
            ltp = None
            if resp and resp.get("status") == "success":
                data = resp.get("data") or {}
                if isinstance(data, dict) and 'data' in data:
                    data = data.get('data')
                idx_data = (data or {}).get(seg, {}).get(str(sec), {})
                ltp = idx_data.get("last_price") if isinstance(idx_data, dict) else None

            if ltp is not None:
                r.publish(f"ltp:{idx}", json.dumps({"ltp": float(ltp), "ts": now_ts}))

            # Fetch option chain and publish OC
            try:
                oc_resp = client.option_chain(under_security_id=sec, under_exchange_segment=seg)
            except Exception:
                oc_resp = None

            oc = {}
            expiry = None
            if oc_resp and oc_resp.get("status") == "success":
                data = oc_resp.get("data") or {}
                if isinstance(data, dict) and 'data' in data:
                    data = data.get('data')
                # Extract oc payload
                if isinstance(data, dict):
                    oc = data.get('oc') or {}
                expiry = oc_resp.get('expiry') or oc_resp.get('data', {}).get('expiry')
                # Publish raw option chain
                try:
                    key = f"oc:{idx}:{expiry or 'unknown'}"
                    payload = json.dumps(oc)
                    # publish for subscribers and store latest as key for HTTP access
                    r.publish(key, payload)
                    try:
                        r.set(f"oc_latest:{idx}:{expiry or 'unknown'}", payload)
                    except Exception:
                        pass
                    # also persist into Postgres (upsert)
                    try:
                        ensure_pg()
                        if pg_conn:
                            cur = pg_conn.cursor()
                            cur.execute(
                                "INSERT INTO option_chains (idx, expiry, payload, updated_at) VALUES (%s, %s, %s, CURRENT_TIMESTAMP) "
                                "ON CONFLICT (idx, expiry) DO UPDATE SET payload = EXCLUDED.payload, updated_at = CURRENT_TIMESTAMP",
                                (idx, expiry or 'unknown', json.dumps(oc)),
                            )
                            pg_conn.commit()
                            cur.close()
                    except Exception:
                        logging.exception("Failed to persist option chain into Postgres")
                except Exception:
                    pass

            # Collect option security ids and fetch their LTPs in batch
            fno_segment = cfg.get("fno_segment")
            sec_ids = []
            if isinstance(oc, dict):
                for k, node in oc.items():
                    if isinstance(node, dict):
                        ce = node.get('ce') or {}
                        pe = node.get('pe') or {}
                        cid = _extract_security_id(ce)
                        pid = _extract_security_id(pe)
                        if cid:
                            try:
                                sec_ids.append(int(cid))
                            except Exception:
                                pass
                        if pid:
                            try:
                                sec_ids.append(int(pid))
                            except Exception:
                                pass

            if sec_ids and fno_segment:
                # Batch fetch option quotes
                try:
                    quote_resp = client.quote_data({fno_segment: sec_ids})
                except Exception:
                    quote_resp = None

                if quote_resp and quote_resp.get('status') == 'success':
                    qdata = quote_resp.get('data') or {}
                    if isinstance(qdata, dict) and 'data' in qdata:
                        qdata = qdata.get('data')
                    fno_map = (qdata or {}).get(fno_segment, {})
                    for sid_str, payload in (fno_map or {}).items():
                        try:
                            sid = int(sid_str)
                        except Exception:
                            try:
                                sid = int(str(sid_str))
                            except Exception:
                                continue
                        l = None
                        if isinstance(payload, dict):
                            l = payload.get('last_price')
                        if l is not None:
                            r.publish(f"ltp:SEC_{sid}", json.dumps({"ltp": float(l), "ts": now_ts}))

        except Exception as e:
            logging.exception(f"Error publishing index {idx}: {e}")


def main():
    if dhanhq is None:
        logging.error("Dhan SDK not installed; feed service cannot run")
        return

    client = dhanhq(DHAN_CLIENT_ID, DHAN_ACCESS_TOKEN)

    poll = float(os.getenv('FEED_POLL_SECONDS', 1.0) or 1.0)
    poll = max(0.25, min(5.0, poll))

    while True:
        try:
            publish_index_and_options(client)
        except Exception:
            logging.exception("Feed loop error")
        time.sleep(poll)


if __name__ == '__main__':
    main()
