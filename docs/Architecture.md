# UrbanFlow Architecture

## Implemented end-to-end flow

```text
NYC TLC public data
        |
        v
Python acquisition
        |
        v
immutable raw files in ADLS Gen2
        |
        v
Databricks / Delta Lake: Bronze -> Silver -> Gold
        |
        v
Snowflake: LANDING -> ANALYTICS + AUDIT
        |
        v
dbt Core: sources -> staging -> intermediate -> marts
        |
        v
URBANFLOW.DBT_DEV analytical views
        |
        v
analytics / future BI consumption
```

This is the primary, implemented architecture. It is production-oriented and live-validated through Phase 8, but it is not a continuously scheduled production deployment. Microsoft Azure is the only cloud platform in scope; AWS is not part of UrbanFlow.

## Component responsibilities

### Public source data

NYC TLC supplies the validated Yellow Taxi monthly Parquet file and taxi-zone reference CSV. An optional NOAA/NCEI client is implemented for local acquisition, but weather is not present in the validated Medallion, Snowflake, or dbt analytical contracts.

### Python acquisition

The local package builds official source URLs, streams downloads to temporary files, atomically publishes completed files, skips existing outputs unless replacement is explicit, and appends sanitized JSONL audit records. Source year/month is configuration-driven.

### Azure Data Lake Storage Gen2

ADLS Gen2 is the durable system of record for immutable source objects and Delta datasets. The Python uploader uses `DefaultAzureCredential` and staged, size-verified, atomic remote publication. Databricks uses Unity Catalog and the existing access connector's managed identity. No storage key, SAS token, connection string, password, service principal, or client secret participates in the validated data path.

### Bronze Delta

Bronze preserves every raw source row and adds file, ingestion, run, and source-period metadata. Content anomalies are observations, not filters. Yellow Taxi uses exact year/month partition replacement; taxi zones use an unpartitioned snapshot. Pipeline and quality evidence are stored as Delta audit datasets.

### Silver Delta

Silver standardizes types and names, creates deterministic trip keys, validates chronology and taxi-zone relationships, and separates valid and rejected records. Quarantine retains all failed rules and the uncast Bronze record. Finite negative monetary values remain identified financial adjustments; null passenger counts remain analytically unknown.

### Gold Delta

Gold retains one row per valid Silver trip, builds deterministic date/time/location dimensions, adds guarded analytical measures, and publishes daily, hourly, and location aggregates. Facts and aggregates use source-period replacement; small dimensions use deterministic snapshots.

### Snowflake

Snowflake serves the seven governed Gold contracts. Databricks writes through `URBANFLOW.LANDING`, executes schema/count/key/boundary/relationship/aggregate gates, then transactionally replaces a bounded partition or full dimension snapshot in `URBANFLOW.ANALYTICS`. Operational evidence is stored in `URBANFLOW.AUDIT`. The two-pass workflow is live-validated for reconciliation and idempotency.

### dbt Core and SQL

Phase 8 declares all seven `ANALYTICS` relations as dbt sources. Seven thin staging views preserve those contracts, `int_trip_enriched` performs six reusable pickup/drop-off dimension joins as an ephemeral model, and four deterministic mart views expose trip, daily, hourly, and location grains. The live `DBT_DEV` build created 11 views, passed all 95 tests, reconciled every source and mart measure to Phase 7, and generated documentation.

### Analytics consumers

`DBT_DEV` views are the validated consumption boundary for analytical queries and a future BI layer. No Power BI semantic model, dashboard, published refresh, or other BI deployment is currently implemented.

## Cross-cutting architecture

- **Identity and secrets:** Azure identity and managed identity are used for lake access; Snowflake key-pair material and the dbt profile remain outside Git.
- **Lineage and audit:** source files, run IDs, ingestion/processing timestamps, schema fingerprints, row counts, quality metrics, load evidence, and reconciliation results connect the major boundaries.
- **Reliability:** temporary publication, Delta ACID writes, exact partition replacement, Snowflake transactions, deterministic snapshots, and fail-closed configuration protect retries.
- **Quality:** schema, required-field, duplicate, invalid-value, relationship, aggregate, and dbt contract checks run at the earliest useful layer.
- **Scope:** orchestration, alerting, BI, CI/CD deployment, and automated cloud provisioning are deferred; they are not part of the diagram above.

## Phase 2 local acquisition boundary

Before Azure provisioning, the implemented local acquisition package proves access patterns against official public sources:

```text
TLC monthly Parquet ─┐
TLC taxi-zone CSV ───┼─→ local Bronze-oriented paths + JSONL audit
NOAA CDO API v2 ─────┘   (optional; token required)
```

TLC files are streamed to temporary files and atomically moved into place. A configured year/month produces exactly one trip-file request. Existing outputs are not downloaded again unless forced. NOAA uses its official token header and paginated `limit`/`offset` requests and combines the returned observations into one raw JSON artifact.

These local paths are the implemented acquisition boundary only; the later ADLS, Databricks, Snowflake, and dbt layers are implemented separately below. They are not created by the acquisition command. ADF and Power BI remain deferred.

## Implemented Phase 3 ADLS Gen2 layer

Phase 3 adds the implemented cloud boundary between local source acquisition and the Azure data lake:

```text
Phase 2 local Bronze files
        ↓
Python ADLS uploader + DefaultAzureCredential
        ↓
ADLS Gen2: urbanflowdata2026 / urbanflow / bronze/...
```

The existing infrastructure was created manually:

- subscription: `Azure for Students`;
- resource group: `rg-urbanflow`;
- HNS-enabled storage account: `urbanflowdata2026`;
- region: Central India;
- filesystem: `urbanflow`; and
- authentication and data access: Microsoft Entra identity with local Azure CLI sign-in.

Application code neither provisions nor configures these resources. It constructs `https://urbanflowdata2026.dfs.core.windows.net`, authenticates through `DefaultAzureCredential`, obtains the existing filesystem client, and creates only the parent directories required by files being uploaded. Storage keys, SAS tokens, connection strings, and passwords are not used.

The implemented cloud paths are:

- `bronze/tlc/yellow/year=2026/month=05/source.parquet`
- `bronze/reference/taxi_zones/taxi_zone_lookup.csv`
- `bronze/weather/year=YYYY/month=MM/observations.json` when a local weather file exists

The uploader does not create `silver/`, `gold/`, or unrelated empty directories. Those downstream paths are owned by the separately implemented Databricks Bronze, Silver, and Gold phases described below. Snowflake and dbt are also implemented separately in Phases 7-8; ADF and Power BI remain deferred.

## Implemented Phase 4 Databricks Bronze layer

Phase 4 separates immutable landed source objects from queryable Bronze Delta datasets:

```text
ADLS raw Bronze (immutable)
├── bronze/tlc/yellow/year=2026/month=05/source.parquet
└── bronze/reference/taxi_zones/taxi_zone_lookup.csv
                 ↓ Azure Databricks / PySpark
ADLS processed Bronze
├── bronze/delta/yellow_taxi/  (_urbanflow_source_year, _urbanflow_source_month)
├── bronze/delta/taxi_zones/   (unpartitioned)
├── audit/bronze_pipeline/     (Delta run records)
└── audit/bronze_quality/      (Delta metric records)
```

The `dbw-urbanflow` Trial workspace in Central India accesses `abfss://urbanflow@urbanflowdata2026.dfs.core.windows.net/` through Unity Catalog. Storage credential `urbanflow_adls_managed_identity` references the existing system-assigned identity on Access Connector `ac-urbanflow`; external location `urbanflow_adls_root` scopes that credential to the existing filesystem. The connector already has Storage Blob Data Contributor on the storage account. No storage key, SAS token, connection string, password, service principal, or client secret participates in this path.

Ingestion adds source-file, UTC ingestion timestamp, run ID, and—on trips—source year/month metadata while preserving source columns. Delta batch replacement gives retry idempotency: one Yellow Taxi period is atomically replaced rather than appended, while the small taxi-zone snapshot is fully replaced without partitions. Data-quality findings remain observational in Bronze. Unreadable or empty input, required-schema loss, and write/count verification failures are critical ingestion failures; reported content anomalies do not remove records.

## Implemented Phase 5 Silver layer

```text
Bronze Delta
├── yellow_taxi/ ── explicit types + business rules + zone joins + deterministic hash
└── taxi_zones/  ── normalized reference contract
                     ↓
Silver Delta
├── fact_trips/              (source_year, source_month)
├── dim_taxi_zones/          (unpartitioned)
├── rejected/trips/          (source_year, source_month)
├── rejected/taxi_zones/     (unpartitioned)
├── audit/silver_pipeline/
└── audit/silver_quality/
```

The fact keeps source lineage, Bronze run ID, a deterministic `trip_id`, Silver run metadata, and an `is_financial_adjustment` flag. Valid trips are checked against the shared Silver taxi-zone contract derived from the actual reference table; zone IDs are never hardcoded. Quarantine keeps standardized fields, all failed rules, the primary rule, a joined reason string, rejection timestamp, lineage, and an uncast Bronze JSON snapshot.

May 2026 live validation on Databricks Serverless produced 4,090,836 valid trips, zero rejected trips, 265 valid zones, and zero rejected zones. The final fact and zone tables have the intended partition strategies, zero duplicate trip IDs, and zero referential-integrity failures. Serverless runs with Unity Catalog external location `urbanflow_adls_root`; no Azure resource or secret-based authentication was added.

## Implemented Phase 6 Gold layer

```text
Silver Delta
+-- fact_trips/ -----------+
+-- dim_taxi_zones/ ---+   |
                       v   v
Gold Delta
+-- fact_trips/             (source_year, source_month)
+-- dim_date/               (unpartitioned)
+-- dim_time/               (unpartitioned, minute grain)
+-- dim_location/           (unpartitioned)
+-- agg_daily_trips/        (source_year, source_month)
+-- agg_location_trips/     (source_year, source_month)
+-- agg_hourly_trips/       (source_year, source_month)
+-- audit/gold_pipeline/
+-- audit/gold_quality/
```

The Gold fact has one row per valid Silver trip and preserves its deterministic key and Bronze/Silver lineage. Date and time foreign keys are derived from pickup/dropoff timestamps; location foreign keys reuse the validated TLC identifiers. Derived measures are guarded against zero, non-finite, and invalid denominators. Financial adjustment rows remain present and are exposed separately from non-adjustment revenue.

Small dimensions use complete deterministic snapshot replacement. The fact and aggregates use exact source-year/month Delta partition replacement, allowing bounded retries without duplicating other batches. Live Serverless validation reconciled 4,090,836 Silver and Gold facts, all three aggregation perspectives, and every dimension relationship with zero critical failures.

## Implemented Phase 7 Snowflake integration

```text
ADLS Gold Delta (managed-identity read)
        |
        | Databricks Serverless + bundled Snowflake Spark connector
        | snowflake_jwt; private PEM retrieved from a Databricks secret scope
        v
URBANFLOW.LANDING
        | schema, key, duplicate, boundary, and row-count gates
        | BEGIN -> scoped DELETE -> INSERT -> COMMIT
        v
URBANFLOW.ANALYTICS
        +--> URBANFLOW.AUDIT.LOAD_AUDIT
```

The connector uses Snowflake's internally managed transfer stage; UrbanFlow creates no external stage, Azure storage integration, SAS token, storage key, service principal, or additional Azure resource. `FACT_TRIPS`, `AGG_DAILY_TRIPS`, `AGG_LOCATION_TRIPS`, and `AGG_HOURLY_TRIPS` replace only a configured source period. `DIM_DATE`, `DIM_TIME`, and `DIM_LOCATION` replace complete validated snapshots. LANDING can contain an incomplete failed transfer, but ANALYTICS remains unchanged unless validation passes and the transaction commits.

The existing service user `URBANFLOW_DATABRICKS_SVC` authenticates with its configured RSA public key. The matching private key remains outside Git and is retrieved at runtime only through configurable secret references under the documented `urbanflow-snowflake` scope. Runtime validation also requires LANDING, ANALYTICS, and AUDIT to be distinct schemas. Databricks job `957309293840081`, run `306537529517430`, completed both passes and final idempotency validation: all seven landing and target counts matched, all 40 reconciliation checks passed, and critical integrity failures were zero.

## Live-validated Phase 8 dbt boundary

```text
URBANFLOW.ANALYTICS (Phase 7 governed source contract)
        |
        | source(); read-only upstream access
        v
staging views (seven complete, lower-case contract interfaces)
        |
        | ref()
        v
int_trip_enriched (ephemeral role-playing dimension joins)
        |
        | ref()
        v
mart_trip_details (view) -----------+
                                      |
staging aggregate views -------------+--> daily/hourly/location mart views
                                      |
                                      v
downstream analytics / future Power BI
```

Phase 8 owns warehouse presentation logic that is genuinely downstream or consumer-specific. Databricks continues to own source standardization, record validity, conformance, deduplication, Gold facts/dimensions/aggregates, and Delta incrementality. Phase 7 continues to own Snowflake landing, ANALYTICS replacement, audits, reconciliation, and idempotency. dbt must not duplicate those transformations without a documented reason or alter validated upstream data to satisfy a test.

Snowflake database/schema access, role permissions, warehouse usage, and authentication remain externally managed prerequisites. The controlled validation used the explicitly prepared `URBANFLOW.DBT_DEV` target, configured `SECURITYADMIN` role, and `COMPUTE_WH`; it did not switch roles or modify grants. `profiles.yml`, the private key, and sensitive values remained outside the repository. Generated `target/`, logs, downloaded packages, and documentation outputs remained outside the repository or ignored. Query-history validation found zero ANALYTICS mutation queries during the validation window.

## Architecture boundaries and deferred capabilities

The current repository supplies acquisition code, ADLS integration, Databricks notebooks, Snowflake SQL/loading utilities, dbt models, and local contract tests. The Azure, Databricks, and Snowflake resources used for validation were prepared externally; the repository does not provision them automatically.

Azure Data Factory was reviewed during Phase 9 and intentionally deferred. No ADF factory, linked service, pipeline, trigger, managed-identity assignment, or successful ADF run is part of the implemented architecture. Power BI, centralized monitoring/alerting, CI/CD deployment, infrastructure-as-code, and analytical weather enrichment are also future enhancements. Phase 10 has not started.
