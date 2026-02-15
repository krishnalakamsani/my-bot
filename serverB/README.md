## Trading Bot v2

Docker Compose stack for an options trading bot where the backend pulls market data (via Dhan) and TimescaleDB stores candles.

### Services

- **frontend** (port 80): React dashboard
- **backend** (port 8001): FastAPI trading engine + WebSocket state + small SQLite config/trades DB
 - **timescaledb** (port 5432): PostgreSQL + Timescale extension (persistent market data)

---

## Quick start

```bash
docker compose up --build
```

Open:

- UI: `http://localhost/`
- Backend status: `http://localhost:8001/api/status`

### Running Server B with Server A

This repo assumes a two-server layout:

- `serverA/` — market-data ingestion: subscribes to Dhan, publishes ticks and option-chains to Redis, and persists 1m candles + option chains to Postgres. It exposes an MDS-compatible HTTP API at `/v1`.
- `serverB/` — trading backend and frontend (this folder). Configure `MDS_BASE_URL` to point to `serverA` (e.g. `http://serverA:8002/v1`).

To run both services locally using the consolidated compose (located in `serverB/docker-compose.yml`), ensure the following env/secrets are set (see `.env.example`):

```bash
# from repository root
docker compose -f serverB/docker-compose.yml up --build
```

Open the UI at `http://localhost:3000` and backend at `http://localhost:8000`.

---

## Architecture and data flow

### 1) Credentials flow (daily token update)

1. Frontend Settings updates **Dhan client id** + **access token**.
2. Backend stores them in SQLite at `backend/data/trading.db` (table: `config`).

This avoids manually updating env vars every day; the backend will use stored credentials to fetch quotes.

### 2) Market data flow

1. Backend fetches index quotes directly via the Dhan SDK (or configured provider) and may build/persist candles to TimescaleDB.
2. Candles are written to TimescaleDB when enabled.
3. Backend runs indicators + entry/exit logic and broadcasts live state to the frontend via WebSocket.

### 3) “Consume-only” backend behavior

The backend is configured (by default in `docker-compose.yml`) to:

- consume candles from MDS (`MARKET_DATA_PROVIDER=mds`)
- avoid persisting tick/candle telemetry to its SQLite DB (`STORE_TICK_DATA=false`, `STORE_CANDLE_DATA=false`)
- prune any leftover tick/candle tables on startup (`PRUNE_DB_ON_STARTUP=true`)

The backend SQLite DB remains small and operational (config + trades).

---

## Persistence

### TimescaleDB

- Candle data persists in a named Docker volume: `timescale_data`.
- You can safely restart containers without losing candle history.

### Backend SQLite

- Stored in the repo at `backend/data/trading.db` and mounted into the backend container at `/app/data`.
- Intended to stay small (config + trades). Candle/tick telemetry is disabled by default.

---

## Market data endpoints

When using an external provider the backend will request candles via its configured API. For local setups the backend pulls directly via Dhan SDK. Consumption code lives in `backend/mds_client.py` (HTTP consumer) and `backend/mds_client.py` may be adapted for provider endpoints.

---

## Bot startup: no indicator warmup

On bot start, the backend can prefetch the last N candles from MDS and seed indicator state before trading begins. This prevents the “warming up” delay after restarts.

- Config key: `prefetch_candles_on_start` (default: `true`)

This seeding covers:

- SuperTrend + MACD
- Score Engine (when enabled)
- HTF (1-minute) SuperTrend filter seeding when `candle_interval < 60`

---

## Score Engine (MDS) explanation

When `indicator_type=score_mds`, the backend uses `backend/score_engine.py` to compute a deterministic multi-timeframe “Market Direction Score” snapshot on each candle close.

### Outputs

Each snapshot includes:

- `score`: signed strength (trend direction + magnitude)
- `slope`: delta of score vs previous candle (momentum)
- `acceleration`: delta of slope (momentum shift)
- `stability`: standard deviation of recent scores (noise)
- `is_choppy`: chop/regime detection (blocks trading)
- `confidence`: 0..1 (used for sizing/quality gating)
- `direction`: `CE` (bullish), `PE` (bearish), `NONE` (neutral band)
- `ready`: whether all required TF indicators have enough history

### Timeframes

The engine uses two timeframes:

- Base timeframe = your trading candle interval (e.g., 5s)
- Next timeframe = next step in the chain (5→15→30→60→300→900)

The next timeframe is weighted higher.

### Per-timeframe scoring

For each timeframe, the engine scores:

1) **SuperTrend**
- Strong continuation: `+2` (bullish) / `-2` (bearish)
- Fresh flip: `+1` / `-1`
- Too many flips recently: `0` (treated as chop)

2) **MACD line**
- Above zero and rising: `+2`
- Above zero and falling: `+1`
- Below zero and falling: `-2`
- Below zero and rising: `-1`
- Near-flat: `0`

3) **Histogram**
- Positive and expanding: `+2`
- Positive and contracting: `+1`
- Negative and contracting: `-2`
- Negative and expanding: `-1`
- Near-zero: `0`

Timeframe score:

```text
raw = st_score + macd_score + hist_score
weighted = raw * timeframe_weight
```

Total `score` is the sum of weighted scores across the two timeframes.

### Direction

The engine sets a neutral band around 0:

- if `score >= neutral_band` → `direction=CE`
- if `score <= -neutral_band` → `direction=PE`
- else → `direction=NONE`

### Example (simplified)

Assume base=5s (weight 1.0), next=15s (weight 2.0).

- 5s: `st=+2`, `macd=+2`, `hist=+1` → raw `+5` → weighted `+5`
- 15s: `st=+2`, `macd=+1`, `hist=+2` → raw `+5` → weighted `+10`

Total score = `15` → typically `direction=CE` (if above the neutral band).
If the previous score was `12`, slope = `+3` (trend is strengthening).

### Bot gating (high level)

The bot typically requires:

- `ready=true`
- `is_choppy=false`
- `direction != NONE`
- minimum `score` and `slope`
- multi-candle confirmation

---

## Troubleshooting

### MDS has no candles

- Check MDS health: `http://localhost:8002/v1/health`
- Check lag/watermarks: `http://localhost:8002/v1/lag`
- Ensure backend has stored credentials in `backend/data/trading.db` (`config` table)
- Ensure Docker Compose mounts `./backend/data:/shared/backend_data:ro` into MDS

### Backend index LTP is 0

- Check MDS candle endpoint returns candles:
  - `http://localhost:8002/v1/candles/last?symbol=NIFTY&timeframe_seconds=5&limit=5`

---

## Repo layout

- `backend/`: backend API + trading engine
- `frontend/`: dashboard UI
 - `market_data_service/`: (removed) ingestion + candle service was previously a separate component; backend now handles data ingestion.
- `docker-compose.yml`: full stack
