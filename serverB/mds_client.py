import time
from datetime import datetime, timedelta, timezone
from typing import Any

import httpx


_client: httpx.AsyncClient | None = None
_last_fetch_ts_close: float = 0.0
_last_fetch_ts_candle: float = 0.0
_last_price: float | None = None
_last_candle_ts: str | None = None
_last_candle: dict[str, Any] | None = None


def _get_client() -> httpx.AsyncClient:
    global _client
    if _client is None:
        _client = httpx.AsyncClient(timeout=httpx.Timeout(2.5, connect=2.0))
    return _client


async def fetch_latest_close(
    *,
    base_url: str,
    symbol: str,
    timeframe_seconds: int,
    min_poll_seconds: float = 1.0,
) -> tuple[float | None, str | None]:
    """Fetch latest candle close from market-data-service.

    Returns (close_price, candle_ts_iso).
    Uses a small in-process throttle so callers can invoke it frequently.
    """
    global _last_fetch_ts_close, _last_price, _last_candle_ts

    now = time.time()
    min_poll_seconds = float(min_poll_seconds or 1.0)
    if min_poll_seconds < 0.2:
        min_poll_seconds = 0.2

    if (now - _last_fetch_ts_close) < min_poll_seconds:
        return _last_price, _last_candle_ts

    _last_fetch_ts_close = now

    if not base_url:
        return None, None

    url = base_url.rstrip("/") + "/candles/last"
    params = {
        "symbol": str(symbol or "").strip().upper(),
        "timeframe_seconds": int(timeframe_seconds),
        "limit": 2,
    }

    client = _get_client()
    resp = await client.get(url, params=params)
    resp.raise_for_status()
    payload: dict[str, Any] = resp.json() if resp.content else {}
    candles = payload.get("candles") or []
    if not isinstance(candles, list) or not candles:
        return None, None

    last = candles[-1]
    if not isinstance(last, dict):
        return None, None

    close = last.get("close")
    ts = last.get("ts")
    try:
        close_f = float(close)
    except Exception:
        close_f = None

    if close_f is not None and close_f > 0:
        _last_price = close_f
        _last_candle_ts = str(ts) if ts is not None else None

    return _last_price, _last_candle_ts


async def fetch_last_candles(
    *,
    base_url: str,
    symbol: str,
    timeframe_seconds: int,
    limit: int,
) -> list[dict[str, Any]]:
    """Fetch last N candles (ascending) from market-data-service."""
    if not base_url:
        return []

    limit_i = int(limit)
    if limit_i < 1:
        limit_i = 1
    if limit_i > 20000:
        limit_i = 20000

    url = base_url.rstrip("/") + "/candles/last"
    params = {
        "symbol": str(symbol or "").strip().upper(),
        "timeframe_seconds": int(timeframe_seconds),
        "limit": limit_i,
    }

    client = _get_client()
    resp = await client.get(url, params=params)
    resp.raise_for_status()
    payload: dict[str, Any] = resp.json() if resp.content else {}
    candles = payload.get("candles") or []
    if not isinstance(candles, list):
        return []

    out: list[dict[str, Any]] = []
    for row in candles:
        if isinstance(row, dict):
            out.append(row)
    return out


async def fetch_candles_range(
    *,
    base_url: str,
    symbol: str,
    timeframe_seconds: int,
    start_iso: str,
    end_iso: str,
    limit: int = 200000,
) -> list[dict[str, Any]]:
    """Fetch candles in a timestamp range (ascending) from market-data-service."""
    if not base_url:
        return []

    limit_i = int(limit)
    if limit_i < 1:
        limit_i = 1
    if limit_i > 200000:
        limit_i = 200000

    url = base_url.rstrip("/") + "/candles/range"
    params = {
        "symbol": str(symbol or "").strip().upper(),
        "timeframe_seconds": int(timeframe_seconds),
        "start": str(start_iso),
        "end": str(end_iso),
        "limit": limit_i,
    }

    client = _get_client()
    resp = await client.get(url, params=params)
    resp.raise_for_status()
    payload: dict[str, Any] = resp.json() if resp.content else {}
    candles = payload.get("candles") or []
    if not isinstance(candles, list):
        return []

    out: list[dict[str, Any]] = []
    for row in candles:
        if isinstance(row, dict):
            out.append(row)
    return out


async def fetch_candles_for_ist_date(
    *,
    base_url: str,
    symbol: str,
    timeframe_seconds: int,
    date_ist: str,
    limit: int = 200000,
) -> list[dict[str, Any]]:
    """Fetch all candles for an IST date (YYYY-MM-DD) using MDS range.

    Converts IST day boundaries to UTC and fetches that full day.
    """
    date_s = str(date_ist or "").strip()
    if not date_s:
        return []

    try:
        y, m, d = (int(x) for x in date_s.split("-"))
        ist = timezone(timedelta(hours=5, minutes=30))
        start_ist = datetime(y, m, d, 0, 0, 0, tzinfo=ist)
        end_ist = start_ist + timedelta(days=1)
        start_utc = start_ist.astimezone(timezone.utc)
        end_utc_excl = end_ist.astimezone(timezone.utc)
        # The MDS endpoint is inclusive on end (<=). Use 1 microsecond before end.
        end_utc_incl = end_utc_excl - timedelta(microseconds=1)
    except Exception:
        return []

    start_iso = start_utc.replace(microsecond=0).isoformat().replace("+00:00", "Z")
    end_iso = end_utc_incl.isoformat().replace("+00:00", "Z")
    return await fetch_candles_range(
        base_url=base_url,
        symbol=symbol,
        timeframe_seconds=timeframe_seconds,
        start_iso=start_iso,
        end_iso=end_iso,
        limit=limit,
    )


async def fetch_latest_candle(
    *,
    base_url: str,
    symbol: str,
    timeframe_seconds: int,
    min_poll_seconds: float = 1.0,
) -> dict[str, Any] | None:
    """Fetch latest full candle from market-data-service.

    Returns a dict with keys like: ts/open/high/low/close/volume.
    Uses in-process throttling so callers can invoke frequently.
    """
    global _last_fetch_ts_candle, _last_candle, _last_candle_ts, _last_price

    now = time.time()
    min_poll_seconds = float(min_poll_seconds or 1.0)
    if min_poll_seconds < 0.2:
        min_poll_seconds = 0.2

    if (now - _last_fetch_ts_candle) < min_poll_seconds:
        return _last_candle

    _last_fetch_ts_candle = now

    candles = await fetch_last_candles(
        base_url=base_url,
        symbol=symbol,
        timeframe_seconds=timeframe_seconds,
        limit=2,
    )
    if not candles:
        return None

    last = candles[-1]
    if not isinstance(last, dict):
        return None

    ts = last.get("ts")
    close = last.get("close")

    try:
        close_f = float(close)
    except Exception:
        close_f = None

    if close_f is not None and close_f > 0:
        _last_price = close_f
        _last_candle_ts = str(ts) if ts is not None else None
        _last_candle = last
        return _last_candle

    return None


async def fetch_option_chain(*, base_url: str, symbol: str, expiry: str) -> dict[str, Any] | None:
    """Fetch latest option-chain (oc) for an index+expiry from market-data-service."""
    if not base_url:
        return None
    url = base_url.rstrip('/') + '/option_chain'
    params = {'symbol': str(symbol or '').strip().upper(), 'expiry': str(expiry or '')}
    client = _get_client()
    resp = await client.get(url, params=params)
    resp.raise_for_status()
    payload = resp.json() if resp.content else {}
    return payload


async def fetch_quote(*, base_url: str, symbol: str) -> dict[str, Any] | None:
    """Fetch latest quote for a symbol from market-data-service.

    Symbol may be index (NIFTY) or SEC_<id> for options.
    """
    if not base_url:
        return None
    url = base_url.rstrip('/') + '/quote'
    params = {'symbol': str(symbol or '').strip()}
    client = _get_client()
    resp = await client.get(url, params=params)
    resp.raise_for_status()
    payload = resp.json() if resp.content else {}
    return payload
