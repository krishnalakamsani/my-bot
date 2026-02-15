"""FastAPI Server - Thin Controller Layer
Only handles API routes, request validation, and responses.
All business logic is delegated to bot_service and other modules.
"""
from fastapi import FastAPI, APIRouter, WebSocket, WebSocketDisconnect, HTTPException, Query
from fastapi.responses import JSONResponse
from starlette.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
import asyncio
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import List

# Local imports
from config import ROOT_DIR, bot_state, config
import re

from models import ConfigUpdate, StrategyCreate, StrategyRename, StrategyDuplicate, StrategiesImport
from database import (
    init_db,
    load_config,
    prune_backend_market_data,
    get_trades,
    get_trade_analytics,
    upsert_strategy,
    list_strategies,
    get_strategy,
    delete_strategy,
    rename_strategy,
    duplicate_strategy,
    export_strategies,
    import_strategies,
    mark_strategy_applied,
)
import bot_service

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(ROOT_DIR / 'logs' / 'bot.log', mode='a')
    ]
)
logger = logging.getLogger(__name__)

# Reduce noisy per-request logs from http clients (used for MDS polling).
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)

# Ensure directories exist
(ROOT_DIR / 'logs').mkdir(exist_ok=True)
(ROOT_DIR / 'data').mkdir(exist_ok=True)


# WebSocket Connection Manager
class ConnectionManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)
        try:
            client = getattr(websocket, 'client', None)
            logger.info(f"[WS] Client connected: {client} | Total={len(self.active_connections)}")
        except Exception:
            logger.info(f"[WS] Client connected | Total={len(self.active_connections)}")

    def disconnect(self, websocket: WebSocket):
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)
            try:
                client = getattr(websocket, 'client', None)
                logger.info(f"[WS] Client disconnected: {client} | Total={len(self.active_connections)}")
            except Exception:
                logger.info(f"[WS] Client disconnected | Total={len(self.active_connections)}")

    async def broadcast(self, message: dict):
        if not self.active_connections:
            return

        try:
            logger.debug(f"[WS] Broadcasting message type={message.get('type')} to {len(self.active_connections)} clients")
        except Exception:
            logger.debug("[WS] Broadcasting message to clients")

        # Send in a bounded way; drop broken/slow sockets to avoid log spam.
        stale: List[WebSocket] = []
        for connection in list(self.active_connections):
            client = getattr(connection, 'client', None)
            try:
                await asyncio.wait_for(connection.send_json(message), timeout=2)
            except asyncio.TimeoutError as te:
                stale.append(connection)
                try:
                    logger.warning(f"[WS] Broadcast timeout for client={client}; message_type={message.get('type')} size={len(str(message))} chars; dropping client: {te}")
                except Exception:
                    logger.warning(f"[WS] Broadcast timeout; dropping client: {client}")
            except Exception as e:
                stale.append(connection)
                # Log full exception with traceback to diagnose underlying cause (network/proxy/closed socket)
                try:
                    logger.exception(f"[WS] Broadcast failed for client={client}; message_type={message.get('type')} size={len(str(message))} chars; dropping client")
                except Exception:
                    logger.exception(f"[WS] Broadcast failed; dropping client")

        for ws in stale:
            self.disconnect(ws)

        logger.debug("[WS] Broadcast complete")


manager = ConnectionManager()

_market_data_service = None
_mds_consumer_task: asyncio.Task | None = None


async def _mds_consumer_loop():
    """Continuously consume latest candle close from market-data-service.

    This keeps the UI market feed live even when the trading bot is stopped.
    It does not persist anything to backend SQLite.
    """
    try:
        from mds_client import fetch_latest_close
    except Exception as e:
        logger.warning(f"[MDS] Consumer disabled (import error): {e}")
        return

    dhan_fallback = None

    while True:
        try:
            if str(config.get('market_data_provider', '')).strip().lower() != 'mds':
                await asyncio.sleep(2)
                continue

            if not bool(config.get('enable_mds_consumer', True)):
                await asyncio.sleep(2)
                continue

            base_url = str(config.get('mds_base_url', '') or '').strip()
            if not base_url:
                await asyncio.sleep(2)
                continue

            index_name = config.get('selected_index', 'NIFTY')
            tf = int(config.get('candle_interval', 5) or 5)
            poll_s = float(config.get('mds_poll_seconds', 1.0) or 1.0)

            close_price, _ts = await fetch_latest_close(
                base_url=base_url,
                symbol=index_name,
                timeframe_seconds=tf,
                min_poll_seconds=poll_s,
            )

            if close_price is not None and close_price > 0:
                bot_state['index_ltp'] = float(close_price)
            else:
                # Fallback: if MDS has no candles (e.g., not yet backfilled/streaming),
                # keep the UI feed live via Dhan quotes (no persistence).
                if config.get('dhan_access_token') and config.get('dhan_client_id'):
                    try:
                        if dhan_fallback is None:
                            from dhan_api import DhanAPI

                            dhan_fallback = DhanAPI(config.get('dhan_access_token'), config.get('dhan_client_id'))
                        ltp = await asyncio.to_thread(dhan_fallback.get_index_ltp, index_name)
                        if ltp and float(ltp) > 0:
                            bot_state['index_ltp'] = float(ltp)
                    except Exception:
                        pass

            await asyncio.sleep(max(0.5, poll_s))

        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.warning(f"[MDS] Consumer error: {e}")
            await asyncio.sleep(2)


# Lifespan context manager
@asynccontextmanager
async def lifespan(app: FastAPI):
    global _market_data_service, _mds_consumer_task

    await init_db()
    await load_config()
    logger.info(f"[STARTUP] Database initialized, config loaded. Index={config.get('selected_index', 'NIFTY')}, Indicator=SuperTrend")

    # Keep backend SQLite minimal (prune tick/candle tables unless explicitly enabled).
    if bool(config.get('prune_db_on_startup', True)):
        try:
            result = await prune_backend_market_data()
            logger.info(f"[STARTUP] DB prune: {result}")
        except Exception as e:
            logger.warning(f"[STARTUP] DB prune skipped: {e}")

    # Start MDS consumer (consume-only, no backend DB writes) for UI market feed.
    try:
        base_url = str(config.get('mds_base_url', '') or '').strip()
        if (
            str(config.get('market_data_provider', '')).strip().lower() == 'mds'
            and bool(config.get('enable_mds_consumer', True))
            and base_url
        ):
            _mds_consumer_task = asyncio.create_task(_mds_consumer_loop())
            logger.info('[MDS] Consumer started')
        else:
            if str(config.get('market_data_provider', '')).strip().lower() == 'mds':
                logger.info('[MDS] Consumer not started: MDS base URL not configured or consumer disabled')
    except Exception as e:
        logger.warning(f"[MDS] Consumer not started: {e}")

    # Optional legacy: Start independent market-data capture (runs even if bot is stopped).
    # Default OFF when using the separate market-data-service container.
    if bool(config.get('enable_internal_market_data_service', False)) and config.get("dhan_access_token") and config.get("dhan_client_id"):
        try:
            from dhan_api import DhanAPI
            from market_data_service import MarketDataService

            dhan = DhanAPI(config.get("dhan_access_token"), config.get("dhan_client_id"))
            _market_data_service = MarketDataService(dhan)
            await _market_data_service.start()
        except Exception as e:
            _market_data_service = None
            logger.warning(f"[STARTUP] MarketDataService not started: {e}")

    try:
        yield
    finally:
        if _market_data_service is not None:
            try:
                await _market_data_service.stop()
            except Exception:
                pass
            _market_data_service = None

        if _mds_consumer_task is not None:
            try:
                _mds_consumer_task.cancel()
            except Exception:
                pass
            _mds_consumer_task = None

        logger.info("[SHUTDOWN] Server shutting down")


app = FastAPI(lifespan=lifespan)
api_router = APIRouter(prefix="/api")


def _filter_strategy_config(candidate: dict) -> dict:
    """Allow only known config keys; never persist/apply credentials."""
    if not isinstance(candidate, dict):
        return {}

    allowed_keys = set(config.keys())
    disallowed = {"dhan_access_token", "dhan_client_id"}
    allowed_keys -= disallowed

    filtered = {}
    for k, v in candidate.items():
        if k in allowed_keys:
            filtered[k] = v
    return filtered


def _validate_strategy_name(name: str) -> str:
    name = str(name or "").strip()
    if not name:
        raise ValueError("Strategy name is required")
    if len(name) > 60:
        raise ValueError("Strategy name too long (max 60 chars)")
    # Allow letters, numbers, spaces, and a few safe separators
    if not re.match(r"^[A-Za-z0-9 _\-\.\+\(\)\[\]]+$", name):
        raise ValueError("Strategy name contains unsupported characters")
    return name


def _validate_strategy_config(cfg: dict) -> None:
    if not isinstance(cfg, dict):
        raise ValueError("Strategy config must be an object")

    st_period = int(cfg.get("supertrend_period", 7) or 7)
    if st_period < 1 or st_period > 200:
        raise ValueError("supertrend_period out of range")

    st_mult = float(cfg.get("supertrend_multiplier", 4) or 4)
    if st_mult <= 0 or st_mult > 50:
        raise ValueError("supertrend_multiplier out of range")

    macd_fast = int(cfg.get("macd_fast", 12) or 12)
    macd_slow = int(cfg.get("macd_slow", 26) or 26)
    macd_sig = int(cfg.get("macd_signal", 9) or 9)
    if macd_fast < 1 or macd_slow < 1 or macd_sig < 1:
        raise ValueError("MACD periods must be >= 1")
    if macd_fast >= macd_slow:
        raise ValueError("macd_fast must be less than macd_slow")

    ind = str(cfg.get("indicator_type", "score_mds") or "").lower()
    if ind not in ("score_mds",):
        raise ValueError("indicator_type must be 'score_mds'")

    # ADX validation (used by supertrend_adx)
    if "adx_period" in cfg and cfg["adx_period"] is not None:
        v = int(cfg["adx_period"])
        if v < 1 or v > 200:
            raise ValueError("adx_period out of range")

    if "adx_threshold" in cfg and cfg["adx_threshold"] is not None:
        v = float(cfg["adx_threshold"])
        if v < 0 or v > 100:
            raise ValueError("adx_threshold out of range")

    for key in ("min_trade_gap", "min_hold_seconds", "min_order_cooldown_seconds"):
        if key in cfg and cfg[key] is not None:
            v = int(cfg[key])
            if v < 0 or v > 3600:
                raise ValueError(f"{key} out of range")

    if "htf_filter_timeframe" in cfg and cfg["htf_filter_timeframe"] is not None:
        v = int(cfg["htf_filter_timeframe"])
        # Current backend implementation constrains this to 60s.
        if v != 60:
            raise ValueError("htf_filter_timeframe currently supports only 60 seconds")


# ==================== API Routes ====================

@api_router.get("/")
async def root():
    """Health check endpoint"""
    return {"message": "Trading Bot API", "status": "running"}


@api_router.get("/status")
async def get_status():
    """Get bot status"""
    return bot_service.get_bot_status()


@api_router.get("/market/nifty")
async def get_market_data():
    """Get market data (index LTP, SuperTrend)"""
    return bot_service.get_market_data()


@api_router.get("/position")
async def get_position():
    """Get current position"""
    return bot_service.get_position()


@api_router.get("/trades")
async def get_trades_list(limit: int = Query(default=None, le=10000)):
    """Get trade history. Pass limit=None to get all trades"""
    return await get_trades(limit)


@api_router.get("/analytics")
async def get_analytics():
    """Get comprehensive trade analytics and statistics"""
    return await get_trade_analytics()


@api_router.get("/summary")
async def get_summary():
    """Get daily summary"""
    return bot_service.get_daily_summary()


@api_router.get("/logs")
async def get_logs(level: str = Query(default="all"), limit: int = Query(default=100, le=500)):
    """Get bot logs"""
    logs = []
    log_file = ROOT_DIR / 'logs' / 'bot.log'
    
    if log_file.exists():
        with open(log_file, 'r') as f:
            lines = f.readlines()[-limit:]
            for line in lines:
                try:
                    parts = line.strip().split(' - ')
                    if len(parts) >= 4:
                        timestamp = parts[0]
                        log_level = parts[2]
                        message = ' - '.join(parts[3:])
                        
                        if level == "all" or level.upper() == log_level:
                            logs.append({
                                "timestamp": timestamp,
                                "level": log_level,
                                "message": message
                            })
                except Exception:
                    pass
    
    return logs


@api_router.get("/config")
async def get_config():
    """Get current configuration"""
    return bot_service.get_config()


@api_router.post("/debug/ws_test")
async def debug_ws_test(index: str = Query(default=None)):
    """Trigger a test broadcast to connected WebSocket clients.

    Optional query parameter `index` (eg. SENSEX) will fetch the latest close from MDS
    and broadcast a `state_update` with `index_ltp` set to that value for verification.
    """
    try:
        if index:
            # Fetch latest close from MDS for the requested index
            try:
                from mds_client import fetch_latest_close
                base_url = str(config.get('mds_base_url', '') or '').strip()
                if not base_url:
                    raise HTTPException(status_code=400, detail="MDS base URL not configured")
                tf = int(config.get('candle_interval', 5) or 5)
                poll_s = float(config.get('mds_poll_seconds', 1.0) or 1.0)

                close_price, _ts = await fetch_latest_close(
                    base_url=base_url,
                    symbol=index,
                    timeframe_seconds=tf,
                    min_poll_seconds=poll_s,
                )

                if close_price is None:
                    raise HTTPException(status_code=404, detail=f"No close price available for {index}")

                payload = {
                    "type": "state_update",
                    "data": {
                        "index_ltp": float(close_price),
                        "selected_index": index,
                        "timestamp": datetime.now(timezone.utc).isoformat()
                    }
                }

                logger.info(f"[DEBUG] Broadcasting state_update for index={index} LTP={close_price}")
                await manager.broadcast(payload)
                return JSONResponse({"status": "ok", "sent": payload})

            except HTTPException:
                raise
            except Exception as e:
                logger.exception(f"[DEBUG] Failed to fetch close for {index}: {e}")
                raise HTTPException(status_code=500, detail=str(e))

        # Fallback: simple debug test broadcast
        payload = {
            "type": "debug_test",
            "message": "this is a test broadcast",
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
        await manager.broadcast(payload)
        logger.info("[DEBUG] Triggered test broadcast")
        return JSONResponse({"status": "ok", "sent": payload})

    except Exception as e:
        logger.exception(f"[DEBUG] Failed to send test broadcast: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@api_router.get("/indices")
async def get_indices():
    """Get available indices"""
    return bot_service.get_available_indices_list()


@api_router.get("/candles")
async def get_candles(limit: int = Query(default=1000, le=10000), index_name: str = Query(default=None)):
    """Get historical candle data for analysis"""
    from database import get_candle_data
    return await get_candle_data(limit=limit, index_name=index_name)


@api_router.get("/timeframes")
async def get_timeframes():
    """Get available timeframes"""
    return bot_service.get_available_timeframes()


@api_router.post("/config/update")
async def update_config(update: ConfigUpdate):
    """Update configuration"""
    return await bot_service.update_config_values(update.model_dump(exclude_none=True))


@api_router.post("/config/mode")
async def set_mode(mode: str = Query(..., regex="^(paper|live)$")):
    """Set trading mode"""
    result = await bot_service.set_trading_mode(mode)
    if result.get('status') == 'error':
        raise HTTPException(status_code=400, detail=result['message'])
    return result


@api_router.post("/bot/start")
async def start_bot():
    """Start the trading bot"""
    return await bot_service.start_bot()


@api_router.post("/bot/stop")
async def stop_bot():
    """Stop the trading bot"""
    return await bot_service.stop_bot()


@api_router.post("/bot/squareoff")
async def squareoff():
    """Force square off position"""
    return await bot_service.squareoff_position()


# ==================== Strategies ====================

@api_router.get("/strategies")
async def get_strategies():
    """List saved strategies"""
    return await list_strategies()


@api_router.post("/strategies")
async def save_strategy(payload: StrategyCreate):
    """Save a named strategy (config snapshot).

    If payload.config is omitted, uses current backend config snapshot.
    Credentials are never stored.
    """
    name = _validate_strategy_name(payload.name)
    snapshot = payload.config if payload.config is not None else dict(config)
    safe = _filter_strategy_config(snapshot)
    _validate_strategy_config(safe)
    try:
        result = await upsert_strategy(name, safe)
        return {"status": "success", "strategy": result}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@api_router.delete("/strategies/{strategy_id}")
async def remove_strategy(strategy_id: int):
    ok = await delete_strategy(strategy_id)
    if not ok:
        raise HTTPException(status_code=404, detail="Strategy not found")
    return {"status": "success"}


@api_router.patch("/strategies/{strategy_id}")
async def update_strategy_name(strategy_id: int, payload: StrategyRename):
    try:
        new_name = _validate_strategy_name(payload.name)
        result = await rename_strategy(strategy_id, new_name)
        return {"status": "success", "strategy": result}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@api_router.post("/strategies/{strategy_id}/duplicate")
async def duplicate_strategy_api(strategy_id: int, payload: StrategyDuplicate):
    try:
        new_name = _validate_strategy_name(payload.name)
        result = await duplicate_strategy(strategy_id, new_name)
        return {"status": "success", "strategy": result}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@api_router.get("/strategies/export")
async def export_strategies_api():
    return {"strategies": await export_strategies()}


@api_router.post("/strategies/import")
async def import_strategies_api(payload: StrategiesImport):
    # Filter + validate each item
    cleaned = []
    for item in payload.strategies or []:
        if not isinstance(item, dict):
            continue
        try:
            name = _validate_strategy_name(item.get("name"))
        except Exception:
            continue
        safe = _filter_strategy_config(item.get("config") or {})
        try:
            _validate_strategy_config(safe)
        except Exception:
            continue
        cleaned.append({"name": name, "config": safe})

    result = await import_strategies(cleaned)
    return {"status": "success", **result}


@api_router.post("/strategies/{strategy_id}/apply")
async def apply_strategy(strategy_id: int, start: bool = Query(default=False)):
    """Apply a saved strategy to current config. Optionally start the bot."""
    if bot_state.get("is_running"):
        raise HTTPException(status_code=400, detail="Stop the bot before applying a strategy")
    if bot_state.get("current_position"):
        raise HTTPException(status_code=400, detail="Close position before applying a strategy")

    strategy = await get_strategy(strategy_id)
    if not strategy:
        raise HTTPException(status_code=404, detail="Strategy not found")

    safe_updates = _filter_strategy_config(strategy.get("config") or {})
    _validate_strategy_config(safe_updates)
    result = await bot_service.update_config_values(safe_updates)
    if result.get("status") != "success":
        return {"status": "error", "message": "Failed to apply strategy", "result": result}

    await mark_strategy_applied(strategy_id)

    if start:
        start_result = await bot_service.start_bot()
        return {
            "status": "success",
            "message": f"Applied strategy '{strategy.get('name')}' and started bot",
            "strategy": {"id": strategy.get("id"), "name": strategy.get("name")},
            "apply": result,
            "start": start_result,
        }

    return {
        "status": "success",
        "message": f"Applied strategy '{strategy.get('name')}'",
        "strategy": {"id": strategy.get("id"), "name": strategy.get("name")},
        "apply": result,
    }


# ==================== WebSocket ====================

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    """WebSocket endpoint for real-time updates"""
    await manager.connect(websocket)
    try:
        try:
            client = getattr(websocket, 'client', None)
            logger.info(f"[WS] Connection established handler for client: {client}")
        except Exception:
            logger.info("[WS] Connection established handler")

        while True:
            try:
                data = await asyncio.wait_for(websocket.receive_text(), timeout=30)
                if data == "ping":
                    logger.debug(f"[WS] Received ping from {getattr(websocket, 'client', None)}")
                    await websocket.send_text("pong")
            except asyncio.TimeoutError:
                try:
                    hb = {"type": "heartbeat", "timestamp": datetime.now(timezone.utc).isoformat()}
                    logger.debug(f"[WS] Sending heartbeat to {getattr(websocket, 'client', None)}")
                    await websocket.send_json(hb)
                except Exception as e:
                    # Log full exception so we can see why heartbeat failed (closed socket, network error, etc.)
                    logger.exception(f"[WS] Heartbeat send failed for client: {getattr(websocket, 'client', None)}; dropping connection: {e}")
                    break
    except WebSocketDisconnect:
        manager.disconnect(websocket)
    except Exception as e:
        logger.exception(f"[WS] Unexpected error in websocket handler: {e}")
        manager.disconnect(websocket)
    finally:
        manager.disconnect(websocket)


# Include router and middleware
app.include_router(api_router)

app.add_middleware(
    CORSMiddleware,
    allow_credentials=True,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)
