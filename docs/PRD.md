# UrbanFlow Product Requirements Document

## Product definition

UrbanFlow is a real-world data engineering platform that combines NYC Taxi and Limousine Commission (TLC) trip records with a second real source, planned to be historical or observed weather data. It will produce reliable, analytics-ready mobility data through an Azure-only, production-style data platform.

## Business problem

NYC trip records are large, time-varying, and not directly suited to consistent business analysis. Mobility demand is also influenced by time, location, service type, and external conditions such as weather. Analysts need a governed and repeatable way to ingest these sources, standardize them, validate their quality, and expose trusted measures without rebuilding data preparation logic for each question.

## Objectives

- Ingest real TLC and weather data reproducibly and incrementally.
- Preserve source data while creating validated, conformed datasets.
- Build an auditable Bronze, Silver, and Gold data lifecycle.
- Publish a dimensional analytics model to Snowflake.
- Apply dbt transformations, documentation, and tests in the warehouse.
- Orchestrate and monitor pipelines with Azure Data Factory.
- Deliver Power BI-ready datasets for mobility analysis.
- Demonstrate production-minded engineering practices in a portfolio project.

## Target users

- Transportation and city-planning analysts
- Operations and mobility analysts
- Business intelligence developers
- Data engineers and platform reviewers
- Hiring teams evaluating the portfolio implementation

## Major use cases

- Analyze trip volume, revenue, distance, duration, and passenger trends.
- Compare demand by pickup/drop-off zone, date, hour, and service type.
- Evaluate the relationship between weather conditions and trip demand.
- Identify data-quality trends, rejected records, and pipeline health.
- Refresh Power BI reports from governed warehouse models.
- Reprocess a bounded period safely when source data or logic changes.

## Functional requirements

1. Acquire published NYC TLC trip files and data from a selected real weather provider.
2. Land immutable source data in ADLS Gen2 with source and ingestion metadata.
3. Maintain Bronze Delta tables that preserve source fidelity.
4. Transform Bronze data into typed, standardized, deduplicated Silver tables.
5. Produce Gold facts, dimensions, and aggregate datasets.
6. Load curated data into Snowflake incrementally.
7. Use dbt for warehouse transformations, tests, lineage, and documentation.
8. Orchestrate dependencies, retries, and parameterized runs through Azure Data Factory.
9. Record run-level audit metrics, including row counts, timestamps, and status.
10. Surface failures and data-quality results for operational review.
11. Expose stable datasets for Power BI analytics.

## Non-functional requirements

- **Reliability:** Retries and restartable steps must handle transient failures.
- **Idempotency:** Re-running the same partition or batch must not duplicate results.
- **Scalability:** Storage and processing must support multi-month and multi-year TLC volumes.
- **Security:** Secrets must use environment variables locally and managed secret services in cloud environments.
- **Observability:** Every run must be traceable through logs, status, counts, and timestamps.
- **Maintainability:** Code, configuration, schemas, and documentation must be modular and versioned.
- **Data quality:** Critical fields and relationships must be tested at appropriate layers.
- **Cost awareness:** Compute must be right-sized, bounded, and stopped when unused.
- **Reproducibility:** Environments and deployments must be defined through version-controlled configuration.

## Data sources

- **Primary:** Public NYC TLC trip record data and TLC taxi-zone reference data.
- **Secondary:** A real weather source selected during the acquisition phase; no synthetic substitute will be used for production flows.
- Source licenses, retention expectations, schemas, and access constraints will be recorded before ingestion is implemented.

## Expected outputs

- Raw source files and Bronze Delta tables in ADLS Gen2
- Validated Silver trip, zone, and weather datasets
- Gold dimensional models and mobility aggregates
- Snowflake analytics tables and dbt models
- Data-quality results and pipeline audit metadata
- Azure Data Factory orchestration definitions
- Power BI semantic model and dashboard
- Architecture, operations, lineage, and portfolio documentation

## Success criteria

- Real TLC and weather data flow end to end without manual record manipulation.
- Scheduled incremental runs are repeatable and do not create duplicates.
- Bronze records are traceable to their source and ingestion run.
- Silver and Gold datasets pass documented quality thresholds.
- Snowflake and dbt models reconcile with upstream Gold outputs.
- Failed runs are observable and can be safely retried.
- Power BI answers the defined mobility and weather use cases.
- CI validates code, tests, and configuration before integration.
- The final repository accurately documents the implemented system and does not claim unbuilt features.

## Phase 2 acquisition scope

The implemented local acquisition layer uses three real-data categories:

- one configurable monthly Yellow Taxi Parquet file from the [official NYC TLC Trip Record Data page](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page);
- the official TLC Taxi Zone Lookup Table in CSV format; and
- optional daily weather observations from the [NOAA/NCEI Climate Data Online API v2](https://www.ncei.noaa.gov/cdo-web/webservices/v2).

The development default is May 2026, the latest Yellow Taxi month listed when Phase 2 was implemented. Month and taxi type remain configuration-driven to support later incremental ingestion. NOAA live acquisition requires a manually obtained API token; without one, weather is explicitly skipped. Phase 2 stores local Bronze-oriented source files and JSON Lines audit events only. It does not implement cloud landing, Medallion transformations, orchestration, warehouse loading, or analytics.

## Completed Phase 7 warehouse contract

Phase 7 is complete in commit `ea0f67f`. Databricks Serverless job `957309293840081`, successful run `306537529517430`, loaded the seven Phase 6 Gold contracts through `URBANFLOW.LANDING` into governed `URBANFLOW.ANALYTICS` tables and recorded operational evidence in `URBANFLOW.AUDIT`. Both landing passes and final idempotency validation succeeded. The warehouse contains 4,090,836 facts, 6,363 dates, 1,440 minutes, 265 locations, and 35/265/748 daily/location/hourly aggregate rows; duplicate keys, fact relationship failures, partition-boundary failures, aggregate reconciliation failures, audit failures, and all 40 reconciliation failures are zero.

This validated Snowflake contract is the upstream boundary for Phase 8. dbt must consume ANALYTICS without replacing the Databricks Silver/Gold transformations, the Phase 7 loading workflow, or the governed LANDING/ANALYTICS/AUDIT tables.

## Locally implemented Phase 8 dbt milestone

The complete local Phase 8 implementation now includes the constrained dbt Core project, safe
externalized profile contract, seven ANALYTICS sources, seven conservative staging views, one
ephemeral trip-enrichment model, four BI-facing mart views, focused generic/singular tests, static
architecture tests, and synchronized repository documentation.

The implemented presentation contracts are:

- `mart_trip_details`, at one validated trip per row, for detailed service, payment, passenger,
  duration, distance, revenue, and pickup/drop-off analysis;
- `mart_daily_mobility`, at source year/month and pickup-date grain;
- `mart_hourly_mobility`, at source year/month, pickup-date, and hour grain; and
- `mart_location_mobility`, at source year/month and TLC-location grain.

The aggregate marts reuse Phase 7's authoritative daily, hourly, and location measures and add
conformed descriptive attributes. dbt does not recalculate Gold measures, filter financial
adjustments, mutate ANALYTICS, or duplicate Phase 7 audits, partition checks, reconciliation, and
idempotency. No freshness test is invented because no actionable warehouse freshness SLA exists.

Local/offline success means dependency compatibility, `dbt parse`, manifest lineage inspection,
Python tests and compilation, dependency health, diff checks, and security/artifact scans pass
without a Snowflake connection. Full live Phase 8 validation remains pending: a human must approve
the target schema and least-privilege dbt role, provide external key-pair configuration, run the
first `dbt debug`/`dbt build`/documentation generation against Snowflake, and reconcile results to
the Phase 7 evidence. Generated artifacts remain untracked, and no credential or role/account
detail is invented by this implementation.
