Instructions to configure trading-bot-v2-mon to use Server A MDS adapter

1. In the external repo `backend/.env` (or compose env), set:

MARKET_DATA_PROVIDER=mds
MDS_BASE_URL=http://<serverA_private_ip_or_hostname>:8002

2. Ensure the backend container can reach Server A (VPC/private network). Do NOT expose MDS adapter publicly.

3. Optionally disable the backend's direct Dhan WS usage by ensuring Dhan credentials are blank in the backend config or via the UI.

4. Restart backend container.

Notes: The adapter implements `/v1/candles/last` which maps to `mds_client.fetch_last_candles` used in the repo.
