# UrbanFlow

UrbanFlow is a portfolio data engineering platform for analyzing New York City mobility patterns from real NYC Taxi and Limousine Commission (TLC) trip records and a planned real weather source. The project is designed to demonstrate a production-style, end-to-end analytics workflow without using synthetic production data.

## Planned architecture

Real data sources → Azure Data Factory → Azure Data Lake Storage Gen2 → Bronze → Azure Databricks/PySpark and Delta Lake → Silver → Gold → Snowflake → dbt/SQL → Power BI

UrbanFlow follows a Medallion Architecture. Azure is the project's only cloud platform; AWS is not part of the design.

## Technology stack

- Python, SQL, PySpark, and Delta Lake
- Microsoft Azure, ADLS Gen2, Azure Data Factory, and Azure Databricks
- Snowflake and dbt
- GitHub Actions and Docker
- Power BI

## Current status

Phase 2 implements local acquisition for real TLC Yellow Taxi trip data, the official TLC taxi-zone lookup, and optional NOAA/NCEI weather observations. The default development slice is one Yellow Taxi month: May 2026. No Azure, Databricks, Snowflake, ADF, dbt, or Power BI resources or pipelines have been implemented.

## Local acquisition

Official sources: [NYC TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) and [NOAA/NCEI Climate Data Online API v2](https://www.ncei.noaa.gov/cdo-web/webservices/v2).

From the project root, activate `.venv` and run:

```powershell
python -m src.ingestion.run_ingestion --source tlc
python -m src.ingestion.run_ingestion --source taxi-zones
python -m src.ingestion.run_ingestion --source weather
python -m src.ingestion.run_ingestion --source all
```

Configure the month and source settings through environment variables listed in `.env.example`. Existing files are skipped unless `--force` is supplied. Downloads stream to `.part` files and are renamed only after success.

Local raw outputs are ignored by Git:

- `data/bronze/tlc/{taxi_type}/year=YYYY/month=MM/source.parquet`
- `data/bronze/reference/taxi_zones/taxi_zone_lookup.csv`
- `data/bronze/weather/year=YYYY/month=MM/observations.json`
- `data/audit/ingestion_audit.jsonl`

Weather ingestion is optional. Without `NOAA_API_TOKEN`, `weather` and the weather portion of `all` are recorded as skipped without making a NOAA request. Obtaining a NOAA API token is a later manual prerequisite.

See [`docs/Phases.md`](docs/Phases.md) for the implementation roadmap and [`docs/Architecture.md`](docs/Architecture.md) for the intended architecture.
