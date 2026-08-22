# UrbanFlow Architecture

## Intended data flow

```text
Real Data Sources
        ↓
Azure Data Factory
        ↓
Azure Data Lake Storage Gen2
        ↓
Bronze
        ↓
Azure Databricks / PySpark
        ↓
Silver
        ↓
Gold
        ↓
Snowflake
        ↓
dbt / SQL
        ↓
Power BI
```

The platform uses Medallion Architecture to separate source preservation, validated data, and business-ready analytics. Microsoft Azure is the only cloud platform in scope. **AWS is not part of UrbanFlow's architecture or implementation.**

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

Snowflake is the analytical warehouse serving curated models. Gold data will be loaded incrementally into controlled schemas, with reconciliation and load-audit checks between lake and warehouse.

### dbt and SQL

dbt will manage warehouse transformations, dimensional presentation models, tests, lineage, and generated documentation. SQL models will remain modular, reviewable, and environment-aware.

### Power BI

Power BI will consume stable Snowflake models to provide mobility dashboards and interactive analysis. Business measures will be defined consistently and refresh behavior will be documented.

## Cross-cutting concerns

- Identity and secrets will use Azure-managed identities and approved secret stores where supported.
- Audit metadata will connect source files, pipeline runs, Delta versions, and warehouse loads.
- CI/CD will validate repository changes and later deploy environment-specific artifacts safely.
- Storage, compute, and orchestration will be designed for bounded cost and repeatable teardown where appropriate.

## Phase 2 local acquisition boundary

Before Azure provisioning, the implemented local acquisition package proves access patterns against official public sources:

```text
TLC monthly Parquet ─┐
TLC taxi-zone CSV ───┼─→ local Bronze-oriented paths + JSONL audit
NOAA CDO API v2 ─────┘   (optional; token required)
```

TLC files are streamed to temporary files and atomically moved into place. A configured year/month produces exactly one trip-file request. Existing outputs are not downloaded again unless forced. NOAA uses its official token header and paginated `limit`/`offset` requests and combines the returned observations into one raw JSON artifact.

These local paths model the future lake organization but are not ADLS Gen2, Delta Lake, or a cloud Bronze layer. ADF, Azure Databricks, Snowflake, dbt, and Power BI remain planned components.
