# My Bot — Server A (MDS) & Server B (Trading)

This repository contains a two-server trading architecture:

- `serverA/` — Market Data Service (MDS): data ingestion, Redis pub/sub, Postgres persistence, and an MDS-compatible HTTP adapter.
- `serverB/` — Trading backend + frontend: your working bot (strategies, UI, order execution). Configure it to consume market data from Server A when running in production.

Quick overview

- Server A (market-data)
  - Responsibilities: subscribe/poll Dhan for index LTPs and option chains; publish ticks to Redis; build 1m candles and persist per-symbol candles to Postgres; persist option-chain JSON to Postgres; expose MDS-compatible HTTP endpoints.
  - Key files: `serverA/feed_service.py`, `serverA/candle_builder.py`, `serverA/market_data_service.py`, `serverA/docker-compose.yml`.
  - HTTP endpoints (MDS adapter):
    - `GET /v1/health` — health check
    - `GET /v1/candles/last?symbol=&timeframe_seconds=&limit=` — aggregated candles
    - `GET /v1/option_chain?symbol=&expiry=` — latest option-chain (reads Redis/DB)
    - `GET /v1/quote?symbol=` — latest quote for `NIFTY` or `SEC_<id>`

- Server B (trading backend + frontend)
  - Responsibilities: indicators, strategies, trade decision logic, order placement (Dhan SDK or execution API), UI.
  - When configured to use MDS, Server B does NOT call Dhan for market data — it obtains candles, option-chains and quotes from Server A only.
  - Key files: `serverB/server.py`, `serverB/trading_bot.py`, `serverB/mds_client.py`, `serverB/docker-compose.yml`, `serverB/Dockerfile`, `serverB/frontend/`.

How MDS mode works

- Set Server B to use MDS:
  - `MARKET_DATA_PROVIDER=mds`
  - `MDS_BASE_URL=http://<serverA-host>:8002/v1`
- Server B uses `serverB/mds_client.py` to call Server A endpoints for candles, option chains and quotes.

Run (local / dev)

1. Start Server A (runs Redis + Postgres + feed + MDS adapter). From repo root:

```bash
cd serverA
docker compose up --build
```

2. Start Server B (backend + frontend + execution_api + Postgres + Redis — consolidated compose):

```bash
cd serverB
docker compose up --build
```

Notes on secrets & env

- Use Docker secrets (files under `serverA/secrets/` and `serverB/secrets/`) for sensitive values:
  - `redis_password.txt`, `postgres_password.txt`, `dhan_client_id.txt`, `dhan_access_token.txt`
- `serverB/.env.example` shows example env vars. Main ones:
  - `MDS_BASE_URL` (Server A adapter URL)
  - Postgres/Redis credentials (or secrets)

Persistence

- Server A persists 1m candles to Postgres table `candles(symbol, ts, open, high, low, close)`.
- Option chains are upserted into `option_chains(idx, expiry, payload JSONB, updated_at)`.

Health checks & testing

- Verify Server A:
  - `curl http://<serverA-host>:8002/v1/health`
  - `curl 'http://<serverA-host>:8002/v1/candles/last?symbol=NIFTY&timeframe_seconds=60&limit=5'`
- Verify Server B (after start):
  - UI: `http://localhost:3000`
  - Backend: `http://localhost:8000/api` (or the API path in `serverB/server.py`)

Operational notes

- Keep Redis/Postgres private — expose only the HTTP MDS endpoint that Server B needs.
- Tune `FEED_POLL_SECONDS` in `serverA` to control load on Redis/Postgres when fetching option-chains.
- For production, consider using a secret manager and TLS for HTTP endpoints.

Next steps

- I can add a small diagram, or extend README with example API request/response payloads.
- I can also provide a one-line `Makefile` or top-level `docker-compose.yml` that starts both servers together if you prefer.

If you want me to include either, tell me which (diagram, single compose, or API examples) and I'll add it.
