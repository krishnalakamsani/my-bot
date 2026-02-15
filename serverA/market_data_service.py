from fastapi import FastAPI, Query, HTTPException
from fastapi.responses import JSONResponse
import os
import psycopg2
import psycopg2.extras
import redis
import json
from typing import List
from datetime import datetime

app = FastAPI(title="MDS-compatible Market Data Adapter")

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_DB = os.getenv("POSTGRES_DB", "candles")
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")
REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD") or os.getenv("REDIS_PASSWORD_FILE")


def _read_secret_from_file(path):
    try:
        if path and os.path.exists(path):
            return open(path, "r").read().strip()
    except Exception:
        pass
    return None

if REDIS_PASSWORD and os.path.exists(str(REDIS_PASSWORD)):
    REDIS_PASSWORD = _read_secret_from_file(REDIS_PASSWORD)

redis_client = redis.Redis(host=REDIS_HOST, port=6379, password=REDIS_PASSWORD, decode_responses=False)


def get_conn():
    return psycopg2.connect(host=POSTGRES_HOST, dbname=POSTGRES_DB, user=POSTGRES_USER, password=POSTGRES_PASSWORD)


@app.get("/v1/health")
def health():
    try:
        conn = get_conn()
        conn.close()
        return {"status": "ok"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


def aggregate_candles_from_db(symbol: str, timeframe_seconds: int, limit: int) -> List[dict]:
    # Aggregate stored 1m candles into requested timeframe (supports multiples of 60)
    if timeframe_seconds % 60 != 0:
        raise ValueError("Only timeframe_seconds multiples of 60 are supported by this adapter")

    group_minutes = timeframe_seconds // 60

    sql = f"""
    SELECT
      min(ts) AS start_ts,
      first(open, ts) AS open,
      max(high) AS high,
      min(low) AS low,
      last(close, ts) AS close
    FROM candles
    WHERE symbol = %s
    GROUP BY (floor((extract(epoch from ts) / 60) / %s))
    ORDER BY start_ts DESC
    LIMIT %s
    """

    conn = get_conn()
    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cur.execute(sql, (symbol, group_minutes, limit))
        rows = cur.fetchall()
        candles = []
        for r in rows:
            ts = r["start_ts"]
            epoch = int(ts.timestamp())
            candles.append({"t": epoch, "o": float(r["open"]), "h": float(r["high"]), "l": float(r["low"]), "c": float(r["close"])})
        return candles
    finally:
        conn.close()


@app.get("/v1/candles/last")
def get_last_candles(symbol: str = Query("NIFTY"), timeframe_seconds: int = Query(60), limit: int = Query(100)):
    try:
        candles = aggregate_candles_from_db(symbol, timeframe_seconds, limit)
        return JSONResponse(content={"candles": candles})
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/v1/option_chain")
def get_option_chain(symbol: str = Query("NIFTY"), expiry: str = Query(None)):
    """Return latest option-chain payload (oc) for index and expiry.

    Reads the latest published OC from Redis key `oc_latest:<symbol>:<expiry>`.
    """
    if not expiry:
        raise HTTPException(status_code=400, detail="expiry is required")
    try:
        key = f"oc_latest:{symbol.upper()}:{expiry}"
        v = redis_client.get(key)
        if not v:
            raise HTTPException(status_code=404, detail="option chain not found")
        payload = v.decode('utf-8') if isinstance(v, (bytes, bytearray)) else v
        oc = json.loads(payload) if payload else {}
        return JSONResponse(content={"oc": oc, "expiry": expiry})
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/v1/quote")
def get_quote(symbol: str = Query("NIFTY")):
    """Return latest LTP for a symbol.

    For indices: symbol is index name (e.g., NIFTY)
    For options: symbol should be `SEC_<security_id>`
    """
    try:
        key = f"ltp:{symbol}"
        v = redis_client.get(key)
        if not v:
            raise HTTPException(status_code=404, detail="quote not found")
        payload = v.decode('utf-8') if isinstance(v, (bytes, bytearray)) else v
        data = json.loads(payload) if payload else {}
        return JSONResponse(content={"quote": data})
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
