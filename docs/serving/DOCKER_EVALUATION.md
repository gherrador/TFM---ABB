# Docker evaluation

Put sample CSV files in `data/bronze/` and execute from the repository root:

```bash
docker compose up --build
```

No first-run manual full-refresh command is required. The pipeline automatically migrates incompatible Silver/Gold/PostgreSQL contracts and is incremental on subsequent runs.

BI serving object: `analytics.vw_process_daily_kpis`.
