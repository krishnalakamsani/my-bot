import asyncio
"""Market data consumer for Server B.

This module must NOT call Dhan directly. It consumes market data from Server A
over the MDS HTTP API (or Redis if configured). The implementation below
queries `MDS_BASE_URL` for the latest quote and option-chain and updates
`bot_state` accordingly.

If you prefer Redis pub/sub, replace the HTTP client with a Redis subscription
that listens to `ltp:<symbol>` channels.
"""

import asyncio
import logging
from typing import Optional

import httpx

from config import bot_state, config

logger = logging.getLogger(__name__)


class MarketDataService:
    """Consumes market data from Server A (MDS) only.

    This class polls the MDS `/quote` endpoint for the selected index and
    — when needed — `/option_chain` for the active expiry. Poll intervals
    are configurable via `market_data_poll_seconds` in `config`.
    """

    def __init__(self):
        self.running = False
        self.task: Optional[asyncio.Task] = None
        self.client = httpx.AsyncClient(timeout=5.0)
        self.mds_base = config.get("MDS_BASE_URL") or config.get("mds_base_url")

    async def start(self):
        if self.running:
            return
        if not self.mds_base:
            raise RuntimeError("MDS_BASE_URL not configured — Server B must use Server A for market data")
        self.running = True
        bot_state["market_data_service_active"] = True
        self.task = asyncio.create_task(self._loop())
        logger.info("[MKT] MarketDataService (MDS consumer) started")

    async def stop(self):
        self.running = False
        if self.task:
            self.task.cancel()
            self.task = None
        await self.client.aclose()
        bot_state["market_data_service_active"] = False
        logger.info("[MKT] MarketDataService stopped")

    async def _fetch_quote(self, symbol: str) -> Optional[dict]:
        url = f"{self.mds_base.rstrip('/')}/quote"
        try:
            r = await self.client.get(url, params={"symbol": symbol})
            r.raise_for_status()
            return r.json()
        except Exception as e:
            logger.debug("Quote fetch failed: %s", e)
            return None

    async def _fetch_option_chain(self, symbol: str, expiry: str) -> Optional[dict]:
        url = f"{self.mds_base.rstrip('/')}/option_chain"
        try:
            r = await self.client.get(url, params={"symbol": symbol, "expiry": expiry})
            r.raise_for_status()
            return r.json()
        except Exception as e:
            logger.debug("Option-chain fetch failed: %s", e)
            return None

    async def _loop(self):
        poll = float(config.get("market_data_poll_seconds", 1.0) or 1.0)
        poll = max(0.25, min(5.0, poll))

        while self.running:
            try:
                index_name = config.get("selected_index", "NIFTY")

                # Get latest quote from MDS
                q = await self._fetch_quote(index_name)
                if q and isinstance(q, dict):
                    # expected {"symbol":..., "ltp":..., "ts":...}
                    ltp = q.get("ltp")
                    if ltp is not None:
                        bot_state["index_ltp"] = float(ltp)

                # If there's an active position, refresh its option chain or quote
                pos = bot_state.get("current_position")
                if pos:
                    sec = str(pos.get("security_id") or "")
                    expiry = pos.get("expiry") or config.get("default_expiry")
                    if expiry and sec and not sec.startswith("SIM_"):
                        oc = await self._fetch_option_chain(index_name, expiry)
                        if oc:
                            bot_state["option_chain"] = oc

                await asyncio.sleep(poll)

            except asyncio.CancelledError:
                break
            except Exception:
                logger.exception("[MKT] Error polling MDS")
                await asyncio.sleep(2)
