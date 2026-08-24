# UrbanFlow Product Requirements Document

## Product definition

UrbanFlow is an end-to-end cloud data engineering platform for public NYC taxi mobility analytics. It converts bounded NYC TLC source files into traceable Delta Lake and Snowflake contracts, then exposes tested analytical views through dbt.

The delivered product is a production-oriented portfolio implementation, live-validated through Phase 8. It is not a continuously scheduled production service and does not claim an enterprise SLA, automated cloud provisioning, Azure Data Factory, or a deployed BI dashboard.

## Business problem

NYC trip records are high-volume, time-varying, and contain provider-submitted anomalies. They are not directly suited to consistent analytical use. Reviewers and downstream analysts need a reproducible process that preserves the source, applies explicit validity rules, publishes stable facts/dimensions/aggregates, proves boundary counts, and makes retries safe.

## Product goals

- Acquire real public TLC data reproducibly for an explicit source period.
- Preserve immutable source records and end-to-end lineage.
- Implement a Bronze, Silver, and Gold Delta lifecycle on Azure Databricks.
- Make invalid data, quality warnings, and audit outcomes visible.
- Support bounded, idempotent reprocessing without duplicate accumulation.
- Publish governed Gold contracts transactionally to Snowflake.
- Use dbt for downstream SQL modeling, tests, lineage, and documentation.
- Demonstrate secure credential boundaries and automated local validation.
- Present all implemented and deferred capabilities accurately for portfolio review.

## Target users

- Data engineers and analytics engineers reviewing architecture and implementation.
- Mobility, operations, and city-planning analysts consuming curated contracts.
- BI developers building a future semantic/reporting layer.
- Recruiters and interviewers assessing a practical data engineering portfolio.

## Implemented use cases

- Analyze validated trip volume, distance, duration, passenger, and monetary measures.
- Compare mobility by pickup/drop-off date, minute/hour, and taxi zone.
- Query daily, hourly, and location aggregate grains.
- Inspect quality anomalies, rejected-row rules, and pipeline audit evidence.
- Reprocess one source month safely and verify stable cardinality.
- Reconcile Delta, Snowflake, and dbt boundaries.

Weather-impact analysis and a published Power BI experience are not implemented use cases.

## Implemented functional requirements

1. Acquire one configurable official TLC Yellow Taxi monthly Parquet file and the official taxi-zone CSV.
2. Optionally acquire NOAA/NCEI observations when an external API token is supplied; skip safely otherwise.
3. Publish local and ADLS files with temporary staging, verification, atomic rename, and audit events.
4. Authenticate to ADLS with `DefaultAzureCredential` and use managed-identity-backed Unity Catalog access in Databricks.
5. Preserve source rows in Bronze Delta and add file, run, ingestion-time, and source-period metadata.
6. Apply explicit Silver schemas, deterministic trip keys, reference-driven location validation, and structured rejected-row handling.
7. Publish a Gold fact, date/time/location dimensions, and daily/hourly/location aggregates with guarded derived measures.
8. Replace facts and aggregates by `source_year` / `source_month` and rebuild small reference dimensions deterministically.
9. Load seven Gold contracts through Snowflake `LANDING` and transactionally publish validated data to `ANALYTICS`.
10. Record load and reconciliation evidence in Snowflake `AUDIT` and prove two-pass idempotency.
11. Treat the seven `ANALYTICS` relations as read-only dbt sources and publish tested views only to `DBT_DEV`.
12. Generate dbt lineage and documentation without committing generated artifacts.
13. Provide PySpark-free local unit and contract tests for deterministic implementation behavior.

## Implemented non-functional requirements

- **Reliability:** temporary publication, Delta ACID writes, Snowflake transactions, fail-closed configuration, and bounded retries where implemented.
- **Idempotency:** exact source-period replacement and deterministic snapshot rebuilding; no blind append at retry-sensitive boundaries.
- **Data quality:** schema, required-field, invalid-value, duplicate, relationship, aggregate, and dbt contract checks with warning/failure semantics.
- **Reconciliation:** source, valid, rejected, Gold, landing, target, and mart counts plus important aggregate measures reconcile.
- **Security:** credentials and private keys remain external; Azure storage access uses identity; no storage key, SAS, password, PAT, or connection string is committed.
- **Traceability:** run IDs, timestamps, source files, source periods, schema fingerprints, counts, status, and sanitized errors are retained where appropriate.
- **Maintainability:** transformations, I/O, configuration, tests, SQL contracts, and documentation are modular and versioned.
- **Cost awareness:** live processing used bounded Databricks Serverless jobs and one bounded monthly dataset; continuous cloud execution is not claimed.
- **Reproducibility:** local Python setup and commands are documented; cloud execution requires pre-existing resources and approved external configuration.

## Data sources

- **Primary:** public NYC TLC Yellow Taxi trip records.
- **Reference:** public NYC TLC Taxi Zone Lookup.
- **Optional acquisition:** NOAA/NCEI Climate Data Online daily observations.

The live analytical validation used May 2026 Yellow Taxi data and the taxi-zone reference. NOAA weather is not joined into Bronze, Silver, Gold, Snowflake, or dbt models.

## Implemented outputs

### Lakehouse

- Immutable TLC trip and taxi-zone source objects in ADLS Gen2.
- Bronze Delta: Yellow Taxi and taxi-zone datasets plus pipeline/quality audits.
- Silver Delta: `fact_trips`, `dim_taxi_zones`, rejected trips/zones, and audits.
- Gold Delta: `fact_trips`, three dimensions, three aggregates, and audits.

### Snowflake

```text
URBANFLOW
├── LANDING
├── ANALYTICS
└── AUDIT
```

Seven Gold contracts are present in `ANALYTICS` after validation. `AUDIT` stores load and reconciliation evidence. `LANDING` isolates transfer and validation from target publication.

### dbt

- 7 governed sources.
- 7 staging views.
- 1 ephemeral intermediate model (`int_trip_enriched`).
- 4 analytical mart views.
- 95 generic/singular tests and generated documentation/lineage.

## Acceptance evidence

- 4,090,836 trip rows retained and reconciled from Bronze through the dbt trip-detail mart.
- 265 taxi-zone/location rows.
- 6,363 date rows and 1,440 time rows.
- 35 daily, 748 hourly, and 265 location aggregate rows.
- Phase 7: two stable loading passes, 40 reconciliation checks passed, and zero critical validation failures.
- Phase 8: 7 sources, 12 models, 95 tests, 11 views, and 106/106 build resources completed.
- Standalone dbt tests: 95/95 passed.
- Current complete local test suite: 125 pytest tests passed.
- Python compilation, dependency, whitespace, secret/artifact, and ignored-data checks passed.
- Query-history validation found no Phase 8 mutations to `URBANFLOW.ANALYTICS`.

## Deferred and future requirements

The following are outside the delivered product and must not be described as implemented:

1. Azure Data Factory or another centralized cloud orchestration/scheduling layer.
2. Power BI semantic model, dashboard, workspace publication, and refresh schedule.
3. Continuous monitoring, alert destinations, operational SLAs, and production runbooks.
4. CI/CD validation and cloud deployment workflows.
5. Automated cloud infrastructure provisioning and environment promotion.
6. Weather enrichment in downstream analytical models.
7. Multi-period performance tuning, workload scaling evidence, and production cost controls.

Phase 9 finalized this scope decision: ADF was evaluated and intentionally deferred because it would add orchestration infrastructure and recurring cloud-operation scope without materially expanding the core data-engineering capabilities already validated. No ADF resource or run exists. Phase 10 has not started.

## Product constraints

- Use real public data; never present synthetic fixtures as production evidence.
- Preserve raw data and validated pipeline semantics.
- Do not weaken data-quality checks or modify governed source data to make downstream tests pass.
- Keep `ANALYTICS` and `DBT_DEV` conceptually and operationally separate.
- Do not fabricate cloud IDs, deployed services, schedules, validation runs, or result metrics.
- Do not expose or commit credentials, private keys, populated profiles, local data, logs, or generated artifacts.
