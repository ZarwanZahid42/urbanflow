# UrbanFlow Architecture

## Intended data flow

```text
Real TLC data
      |
      v
Azure / ADLS Gen2: immutable sources and Bronze storage
      |
      v
Databricks / Delta: Bronze -> Silver -> Gold
      |
      v
Snowflake: LANDING -> validated ANALYTICS (+ AUDIT evidence)
      |
      v
dbt Core (Phase 8 sources/staging implemented; marts planned)
      |
      v
Downstream analytics / Power BI (future)
```

Azure Data Factory remains the planned orchestration layer around this flow; it does not own transformation logic. The platform uses Medallion Architecture to separate source preservation, validated data, and business-ready analytics. Microsoft Azure is the only cloud platform in scope. **AWS is not part of UrbanFlow's architecture or implementation.**

## Component responsibilities

### Real data sources

NYC TLC supplies trip records and taxi-zone reference data. A real weather provider will supply observations needed to analyze weather-related mobility patterns. Source contracts, access methods, and update cadence will be documented before implementation.

### Azure Data Factory

Azure Data Factory (ADF) coordinates data movement and pipeline execution. It will parameterize source periods, trigger ingestion and Databricks jobs, enforce dependencies, apply retry policies, and publish run status. ADF is the orchestration layer, not the primary transformation engine.

### Azure Data Lake Storage Gen2

ADLS Gen2 is the durable system of record for lake data. It will store landed source files and Medallion datasets in separated, access-controlled paths. Directory layout will encode source, entity, and processing date where useful without exposing secrets.

### Bronze

Bronze preserves source fidelity. Records receive ingestion metadata such as source file, batch identifier, ingestion timestamp, and schema version. Transformations are limited to what is required for reliable persistence and traceability.

### Azure Databricks and PySpark

Databricks provides scalable processing with PySpark and Delta Lake. Jobs will validate schemas, standardize types and values, quarantine invalid records, deduplicate data, perform joins, and build Silver and Gold outputs.

### Silver

Silver contains cleaned and conformed trip, zone, calendar, and weather data. It standardizes timestamps, units, identifiers, and null handling while enforcing documented data-quality rules.

### Gold

Gold contains analytics-ready facts, dimensions, and aggregates. Models will support trip performance, geographic demand, time trends, revenue, and weather-impact analysis with stable business definitions.

### Snowflake

Snowflake is the analytical warehouse serving the seven Phase 6 Gold contracts. Phase 7 uses the Databricks Serverless Snowflake Spark connector with key-pair authentication and Snowflake-managed internal transfer staging. Data first lands in `URBANFLOW.LANDING`, passes count/schema/key/boundary checks, and then replaces a bounded partition or dimension snapshot transactionally in `URBANFLOW.ANALYTICS`; operational evidence is written to `URBANFLOW.AUDIT`. The complete two-pass workflow is live-validated, including reconciliation and idempotency gates.

### dbt and SQL

Phase 8 uses dbt Core after `URBANFLOW.ANALYTICS`. Source group `urbanflow_analytics` now declares all seven governed relations, and seven staging views expose their complete contracts with lower-case column names and no filtering, aggregation, or business-rule changes. The aggregate source names `agg_daily`, `agg_location`, and `agg_hourly` map through `identifier` to the physical Phase 7 `*_TRIPS` tables. Contract-backed source and staging tests cover guaranteed keys and fact-to-dimension relationships; freshness remains unset because no authoritative warehouse SLA exists. Intermediate models, marts, generated documentation, and a live Snowflake connection are not implemented yet.

### Power BI

Power BI will consume stable Snowflake models to provide mobility dashboards and interactive analysis. Business measures will be defined consistently and refresh behavior will be documented.

## Cross-cutting concerns

- Identity and secrets will use Azure-managed identities and approved secret stores where supported.
- Audit metadata will connect source files, pipeline runs, Delta versions, and warehouse loads.
- CI/CD will validate repository changes and later deploy environment-specific artifacts safely.
- Storage, compute, and orchestration will be designed for bounded cost and repeatable teardown where appropriate.
- dbt authentication will use environment variables or externally managed credentials; populated profiles, private keys, tokens, account identifiers, and generated dbt artifacts will not be committed.

## Phase 2 local acquisition boundary

Before Azure provisioning, the implemented local acquisition package proves access patterns against official public sources:

```text
TLC monthly Parquet ─┐
TLC taxi-zone CSV ───┼─→ local Bronze-oriented paths + JSONL audit
NOAA CDO API v2 ─────┘   (optional; token required)
```

TLC files are streamed to temporary files and atomically moved into place. A configured year/month produces exactly one trip-file request. Existing outputs are not downloaded again unless forced. NOAA uses its official token header and paginated `limit`/`offset` requests and combines the returned observations into one raw JSON artifact.

These local paths model the future lake organization but are not ADLS Gen2, Delta Lake, or a cloud Bronze layer. ADF, Azure Databricks, Snowflake, dbt, and Power BI remain planned components.

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

The uploader does not create `silver/`, `gold/`, or unrelated empty directories. Those downstream paths are owned by the separately implemented Databricks Bronze, Silver, and Gold processing phases described below. ADF, Snowflake, dbt, and Power BI remain planned and unimplemented.

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

## Initialized Phase 8 dbt boundary

```text
URBANFLOW.ANALYTICS (Phase 7 governed source contract)
        |
        | dbt source() declarations; read-only upstream access
        v
staging models (thin renaming, typing assertions, reusable source interface)
        |
        | ref() dependencies
        v
marts/presentation (declared business grain and consumer purpose)
        |
        v
downstream analytics / future Power BI
```

Phase 8 owns warehouse presentation logic that is genuinely downstream or consumer-specific. Databricks continues to own source standardization, record validity, conformance, deduplication, Gold facts/dimensions/aggregates, and Delta incrementality. Phase 7 continues to own Snowflake landing, ANALYTICS replacement, audits, reconciliation, and idempotency. dbt must not duplicate those transformations without a documented reason or alter validated upstream data to satisfy a test.

Snowflake database/schema access, role permissions, warehouse usage, and authentication are manual prerequisites if the existing access is insufficient. The eventual dbt target schema and least-privilege role must be selected explicitly; this design does not invent either. `profiles.yml` and sensitive values remain external or ignored, with environment-variable or approved secret-manager resolution. Generated `target/`, logs, downloaded packages, and documentation outputs remain untracked. Source declarations and local staging implementation parse offline; no intermediate model, mart, or live Snowflake execution is claimed.
