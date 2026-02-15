MDS Adapter (market_data)

Endpoints:
- GET /v1/health -> {"status":"ok"}
- GET /v1/candles/last?symbol=NIFTY&timeframe_seconds=60&limit=100 -> {"candles": [{"t": epoch, "o":..., "h":..., "l":..., "c":...}, ...]}

Notes:
- Adapter reads 1m candles from `candles` Postgres table and aggregates into requested timeframe when the timeframe is a multiple of 60 seconds.
- Keep this service internal to the private network; configure Server B to use `MARKET_DATA_PROVIDER=mds` and `MDS_BASE_URL=http://<serverA_private_ip>:8002`.
