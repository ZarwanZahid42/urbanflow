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

Phase 5 Silver processing is implemented and live-validated on Unity Catalog Serverless compute. The May 2026 Bronze batch reconciles to 4,090,836 analytics-ready Silver trips and 265 taxi-zone dimension rows with zero quarantined records, duplicates, or referential-integrity failures. Expected source observations remain visible as quality warnings. Unity Catalog uses the existing `ac-urbanflow` managed identity; no keys, SAS tokens, connection strings, passwords, or client secrets are used. Gold, Snowflake, ADF, dbt, and Power BI remain unimplemented.

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

## ADLS Gen2 upload

The Azure uploader uses `DefaultAzureCredential`, which reuses the current Microsoft Entra/Azure CLI identity during local development. It never uses storage account keys or connection strings.

```powershell
python -m src.azure_storage.uploader --source tlc
python -m src.azure_storage.uploader --source taxi-zones
python -m src.azure_storage.uploader --source weather
python -m src.azure_storage.uploader --source all
```

The default command uploads all available files for the configured month. Missing weather is skipped. Existing remote files with the same size are skipped, while size conflicts require explicit review and `--overwrite`.

Implemented cloud layout:

```text
urbanflow/
└── bronze/
    ├── tlc/yellow/year=2026/month=05/source.parquet
    └── reference/taxi_zones/taxi_zone_lookup.csv
```

Uploads use bounded chunks and a temporary remote file. The final path appears only after staged size verification and atomic rename. Upload outcomes are stored locally in `data/audit/azure_upload_audit.jsonl`.

See [`docs/Phases.md`](docs/Phases.md) for the implementation roadmap and [`docs/Architecture.md`](docs/Architecture.md) for the intended architecture.

## Databricks Bronze processing

Phase 4 source notebooks are under `notebooks/bronze/`; reusable, locally testable contracts are under `notebooks/utilities/`. They run in Azure Databricks and intentionally do not add PySpark to the local environment.

```text
immutable raw ADLS objects                 processed Bronze Delta
bronze/tlc/yellow/year=2026/month=05/  ->  bronze/delta/yellow_taxi/
bronze/reference/taxi_zones/           ->  bronze/delta/taxi_zones/
                                            audit/bronze_pipeline/
                                            audit/bronze_quality/
```

Yellow Taxi retries replace only the requested `_urbanflow_source_year` / `_urbanflow_source_month` partition. Taxi zones are a small unpartitioned snapshot and use full replacement. Neither path overwrites the raw source objects. Quality checks report anomalies without filtering or correcting source records. See `docs/Design.md` for thresholds and `docs/Phases.md` for the Databricks workspace setup and validation status.

### Phase 4 validation result

The final Serverless validation reconciled 4,090,836 Yellow Taxi raw rows to 4,090,836 Delta rows and 265 taxi-zone raw rows to 265 Delta rows. Yellow Taxi is partitioned by `_urbanflow_source_year` and `_urbanflow_source_month`; taxi zones are unpartitioned. Two successful Yellow Taxi ingestion audits prove the retry retained stable cardinality.

Bronze quality status is `WARNING`, as expected for preserved real TLC data: 14,231 rows have negative fare amounts and 14,877 have negative total amounts. Duplicate, invalid-timestamp, negative-passenger, and all requested null counts are zero. These findings are reported without removing or modifying Bronze records.

## Silver processing

Phase 5 transforms Bronze Delta into explicitly typed, snake-case Silver Delta datasets:

```text
bronze/delta/yellow_taxi/  ->  silver/fact_trips/
                              silver/rejected/trips/
bronze/delta/taxi_zones/   ->  silver/dim_taxi_zones/
                              silver/rejected/taxi_zones/
                              audit/silver_pipeline/
                              audit/silver_quality/
```

Trips use deterministic SHA-256 fingerprints over standardized business columns and exact year/month partition replacement. Taxi zones use unpartitioned snapshot replacement. Rejected rows retain structured rule arrays and an uncast Bronze JSON payload; no record is silently discarded.

Final Serverless validation reconciled all 4,090,836 Bronze trips to 4,090,836 valid Silver rows and all 265 Bronze zones to 265 valid dimension rows. Two fact runs produced stable counts, zero duplicate trip IDs, and zero pickup/dropoff referential failures. Quality is `WARNING`: 955,371 passenger counts are unknown, 14,231 fare amounts are negative, and 14,877 total amounts are negative. Those finite monetary values are retained as financial adjustments rather than assumed invalid.
