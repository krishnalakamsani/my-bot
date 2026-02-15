# My Bot — Server A (MDS) & Server B (Trading)

This repository contains a two-server trading architecture:

- `serverA/` — Market Data Service (MDS): data ingestion, Redis pub/sub, Postgres persistence, and an MDS-compatible HTTP adapter.
- `serverB/` — Trading backend + frontend: your working bot (strategies, UI, order execution). Configure it to consume market data from Server A when running in production.

Quick overview

- Server A (market-data)
  - Responsibilities: subscribe/poll Dhan for index LTPs and option chains; publish ticks to Redis; build 1m candles and persist per-symbol candles to Postgres; persist option-chain JSON to Postgres; expose MDS-compatible HTTP endpoints.
  - Key files: `serverA/feed_service.py`, `serverA/candle_builder.py`, `serverA/market_data_service.py`, `serverA/docker-compose.yml`.
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

  ## Diagram

  The following Mermaid diagram shows the high-level architecture: Server A ingests market data, persists candles/option-chains and exposes the MDS HTTP API which Server B consumes.

  ```mermaid
  flowchart LR
    subgraph ServerA[Server A - Market Data Service]
      Feed["Feed Service<br/>(Dhan SDK)"]
      RedisA[Redis]
      PgM["Postgres<br/>(candles & option_chains)"]
      Adapter["MDS Adapter<br/>(HTTP API)"]
      Feed --> RedisA
      Feed --> PgM
      RedisA --> Adapter
      PgM --> Adapter
    end

    subgraph ServerB[Server B - Trading Bot]
      Backend["Backend<br/>(uvicorn)"]
      Frontend["Frontend<br/>(React)"]
      Exec[Execution API]
      Backend --> Frontend
      Backend --> Exec
    end

    Adapter -->|HTTP: candles · option_chain · quote| Backend
    RedisA -.->|pubsub optional| Backend

    style ServerA fill:#f9f,stroke:#333,stroke-width:1px
    style ServerB fill:#9ff,stroke:#333,stroke-width:1px
  ```

  Run (local / dev)

  1. Start Server A (runs Redis + Postgres + feed + MDS adapter). From `serverA`:

  ```bash
  cd serverA
  docker compose up --build
  ```

  2. Start Server B (backend + frontend + execution_api + Postgres + Redis). From `serverB`:

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

  - I can also add concrete API examples (cURL / Python) for the MDS endpoints, or export the Mermaid diagram to PNG/SVG. Tell me which and I'll add it.

## Recent Changes & How the Bot Works Now

This project was refactored to an event-driven, hardened trading runtime. The bullets below summarize the important architectural and behavioral changes so you (or an operator) can understand and configure the bot quickly.

- Event-driven execution: strategies publish `ENTRY_SIGNAL` / `EXIT_SIGNAL` events to an in-process `event_bus`. A single execution entry point (`serverB/execution.py`) subscribes and serializes all placements.
- Formal trade lifecycle: `TradingBot` now models trades with a `TradeContext` and deterministic states (e.g. `SIGNAL_GENERATED` → `ENTRY_PENDING` → `ORDER_PLACED` → `POSITION_OPEN` → `EXIT_PENDING` → `CLOSED`). State transitions are driven by `ORDER_PLACED`, `ORDER_FILLED`, `ORDER_TIMEOUT` and `ORDER_SLIPPAGE` events.
- Cross-process atomicity: to avoid double-entries across multiple instances, `execution.py` uses Postgres advisory locks (MD5-based keys derived from `pos_id`) in addition to the existing process-local re-entrant lock.
- Order lifecycle events: execution emits structured lifecycle events: `ORDER_PLACED`, `ORDER_FILLED`, `ORDER_TIMEOUT`, and `ORDER_SLIPPAGE` (useful for monitoring, reconciliation and UI). Each event includes `pos_id` and DB `db_id` when available.
- Pending-order monitor & timeouts: execution tracks pending orders and runs a background monitor that publishes `ORDER_TIMEOUT` if an order isn't filled within `order_timeout_seconds` (configurable).
- Slippage guard: `trading_bot` inspects fills and if fill-price deviation exceeds `max_slippage_pct` it publishes `ORDER_SLIPPAGE` and issues an `EXIT_SIGNAL` to protect capital.
- Risk guard / kill-switches: `trading_bot` enforces pre-execution risk checks and maintains `bot_state` counters. Configurable protections include `daily_max_loss`, `daily_max_loss_pct`, and `consecutive_losses_limit`. When thresholds are breached, `trading_enabled` is set to `False` and new entries are blocked.
- Score engine changes: `serverB/score_engine.py` normalizes raw scores into `entry_score` and `exit_score` (0..1), splits entry/exit confidence, and applies multi-timeframe conflict reduction to reduce noisy cross-timeframe disagreement.
- Position manager & reconciliation (partial): positions are created via the `position_manager` and recorded to the `trades` DB table. Execution emits lifecycle events so a reconciliation loop or external monitor can compare broker state vs internal state and reconcile differences (a reconciliation module is planned as the next step).
- Tests: a concurrency test was added to validate serialization of entry/exit handling and prevent duplicate opens in a single-process environment.

Config knobs (not exhaustive)
- `order_timeout_seconds` — seconds before an order is considered timed-out (default: 30)
- `max_slippage_pct` — allowed slippage percent before triggering exit (default: 0.5)
- `daily_max_loss` / `daily_max_loss_pct` — absolute and percent daily loss limits
- `consecutive_losses_limit` — number of losing trades before disabling trading
- `mds_entry_score_min` / `mds_exit_score_min` — normalized score thresholds used by strategy runner
- `base_lot` — base lot size used by dynamic sizing

Operational notes
- Use the `pos_id` returned by your strategy as a stable trade identifier — it is used to compute advisory locks and join events across modules.
- When running multiple instances / containers, ensure Postgres is shared so advisory locks work across processes.
- The execution background monitor attempts best-effort cancels for timed-out live orders; for full reconciliation, build a separate polling loop against the broker API to verify fills and order states.

Files touched in recent refactor (high level)
- `serverB/execution.py` — single execution entry point, advisory locks, pending-order monitoring, ORDER_* events
- `serverB/trading_bot.py` — `TradeContext`, state machine handlers, risk checks, slippage guard, ORDER_TIMEOUT handler
- `serverB/score_engine.py` — normalized `entry_score`/`exit_score` and MTF conflict reduction
- `serverB/config.py` — added risk & execution configuration keys
- `serverB/tests/run_concurrency_test.py` — concurrency test harness

If you'd like, I can:
- Add a short `OPERATION.md` with runbooks for kill-switch, forced stop and reconciliation commands.
- Implement the execution reconciliation loop (poll broker and reconcile every 5–10s).
- Expand ScoreEngine to the described 0–100 weighted institutional model (weighted MTF, per-indicator buckets and sizing rules).

*** End of recent changes ***

