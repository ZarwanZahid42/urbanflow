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

Phases 6 and 7 are complete and live-validated on Databricks Serverless compute. Phase 7 published all seven Gold contracts to Snowflake with two stable passes, complete reconciliation, and zero critical validation failures. Phase 8 is locally implemented: seven ANALYTICS sources feed seven staging views, one ephemeral trip-enrichment model, and four BI-facing mart views with generic, singular, and static architecture tests. Offline parsing is complete; approved least-privilege setup and the first live Snowflake dbt build remain manual and have not occurred. No private key, password, Azure storage credential, or secret is stored in this repository. ADF and Power BI remain unimplemented.

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

## Gold processing

Phase 6 publishes Delta facts, dimensions, aggregates, and structured audit outputs:

```text
silver/fact_trips/      -> gold/fact_trips/
                           gold/dim_date/
                           gold/dim_time/
silver/dim_taxi_zones/ -> gold/dim_location/
gold/fact_trips/        -> gold/agg_daily_trips/
                           gold/agg_location_trips/
                           gold/agg_hourly_trips/
                           audit/gold_pipeline/
                           audit/gold_quality/
```

The fact retains the Silver `trip_id`, source-period partitions, Bronze/Silver lineage, and every valid trip. It adds date/time keys, duration, speed, fare-per-mile, tip percentage, Gold run lineage, and separate non-adjustment/financial-adjustment revenue measures. Invalid divisions produce null rather than zero, infinity, or NaN; null passenger counts remain unknown.

Final Serverless validation produced 4,090,836 fact rows, 6,363 date rows, 1,440 minute rows, 265 location rows, 35 daily rows, 265 location-aggregate rows, and 748 hourly rows. Every aggregate reconciled to 4,090,836 trips, dimension referential failures and duplicate keys were zero, and repeated partition replacement retained stable fact cardinality. Quality is `WARNING` only for 955,371 null passenger counts and 14,953 financial-adjustment trips.

## Snowflake integration (Phase 7 complete)

The Phase 7 workflow transfers the seven existing Gold Delta contracts through the Databricks Serverless Snowflake Spark connector and Snowflake's internally managed transfer mechanism:

```text
Gold Delta -> URBANFLOW.LANDING -> validated transactional replacement
           -> URBANFLOW.ANALYTICS -> URBANFLOW.AUDIT
```

`FACT_TRIPS` and the three aggregates replace only the requested `source_year` / `source_month`; the three dimensions use deterministic full snapshots. Every batch validates source/landing/target counts, keys, boundaries, fact relationships, aggregate trip totals, audits, and two-pass cardinality. Target changes use explicit Snowflake transactions, so a failed target update is rolled back rather than leaving a partial analytical slice.

Nine ordered notebooks live under `notebooks/snowflake/`. Run `sql/snowflake/01_phase7_tables.sql` first, then configure Databricks-backed scope `urbanflow-snowflake`. The scope and seven key names are notebook widgets, so deployments can override the documented defaults. Data transfer uses the bundled Spark connector; the Snowflake Python connector is used only for validation, audit SQL, and transactional control. UrbanFlow validates the secret as an unencrypted RSA PKCS#8 PEM and converts it to the compact base64 payload expected by the Spark connector without logging the key.

### One-time live configuration

1. Run `sql/snowflake/01_phase7_tables.sql` in Snowflake as the existing loader-capable role.
2. Create the Databricks-backed scope: `databricks secrets create-scope urbanflow-snowflake`.
3. From your own PowerShell session, stream the existing multi-line PEM file directly to the CLI without displaying or copying it into the repository:

   ```powershell
   Get-Content -Raw -LiteralPath 'C:\Users\HS TRADER.urbanflow\secrets\urbanflow_snowflake_key.p8' | databricks secrets put-secret urbanflow-snowflake snowflake_private_key
   ```

4. Populate `snowflake_account`, `snowflake_user`, `snowflake_database`, `snowflake_schema`, `snowflake_warehouse`, and `snowflake_role` with `databricks secrets put-secret ... --string-value ...`. Use `ANALYTICS` for `snowflake_schema`; LANDING and AUDIT are non-secret validated object configuration.
5. Configure the Serverless environment with `snowflake-connector-python>=3.16,<4` and enforce one concurrent Phase 7 run. Pass one shared `run_id` plus `idempotency_pass=1`, run notebooks 01-08, repeat loading/validation with `idempotency_pass=2` and the same `run_id`, then run notebook 09.

The current UrbanFlow environment has completed these one-time prerequisites. Never paste the private key into chat, source code, a notebook, or command history.

### Phase 7 validation result

Databricks job `957309293840081`, run `306537529517430`, completed all 17 tasks successfully, including both landing passes and final idempotency validation. Snowflake reconciled 4,090,836 fact rows, 6,363 dates, 1,440 minutes, 265 locations, 35 daily aggregates, 265 location aggregates, and 748 hourly aggregates. Duplicate keys, fact foreign-key failures, partition-boundary failures, aggregate-total failures, audit failures, and reconciliation failures were all zero. Each dataset recorded two passes with one stable target count.
