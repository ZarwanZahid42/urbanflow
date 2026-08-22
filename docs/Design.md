# UrbanFlow Technical Design

This document describes the intended design. It is not an implementation-status report; the pipelines and external resources described below have not yet been created.

## Source data ingestion

NYC TLC trip files and taxi-zone reference data will be acquired from their official public distribution endpoints. A real weather provider will be selected based on geographic coverage, historical availability, rate limits, licensing, and reproducible access. Ingestion will be parameterized by source, entity, and bounded time period. It will validate response status, file type, size, and where available checksums before making data eligible for processing.

Each run will assign a unique `run_id` and capture source URL/object, source period, retrieval timestamp, file name, byte size, status, and error details. Downloads will use temporary names and become visible at final paths only after successful completion.

## Raw and Bronze storage

ADLS Gen2 will separate immutable landed files from Bronze Delta tables. A prospective layout is:

```text
landing/{source}/{entity}/source_year=YYYY/source_month=MM/
bronze/{source}/{entity}/ingest_date=YYYY-MM-DD/
silver/{domain}/{entity}/
gold/{domain}/{entity}/
quarantine/{source}/{entity}/run_id={run_id}/
```

Exact paths will be finalized after source inspection. Landed objects will not be edited in place. Bronze will preserve source columns and add metadata such as `run_id`, `ingested_at_utc`, `source_file`, `source_period`, and `schema_version`.

## Delta Lake

Bronze, Silver, and Gold lake tables will use Delta Lake for ACID writes, schema controls, scalable merges, and version history. Table properties, retention, optimization, and vacuum settings will be chosen deliberately after volume and recovery requirements are measured. Destructive retention operations will not run by default.

## Partitioning

Partitioning will follow common query and processing boundaries, likely year/month for large trip tables and a date-oriented strategy for weather observations. Low-cardinality or small dimensions will remain unpartitioned. The design will avoid overly granular partitions and will be validated against actual file sizes and query patterns.

## Incremental loading

TLC file periods provide natural batch boundaries. Weather ingestion will use provider timestamps and bounded request windows. A control table will track planned, running, succeeded, and failed batches. A watermark will advance only after required downstream writes and audit records succeed. Backfills will accept explicit start/end periods and reuse normal processing logic.

## Deduplication

The project will first determine whether each source exposes a stable business key. Where it does not, a deterministic hash will be built from documented identifying fields after normalization. Within a batch, records will be ranked deterministically; across batches, Delta `merge` operations will prevent retry duplicates. Duplicate counts and selection rules will be audited rather than silently discarded.

## Schema handling

Source schemas will be captured and versioned. Bronze may preserve newly observed source fields while flagging drift, but required-field removal, incompatible type changes, and unexpected semantic changes will fail or quarantine the batch. Silver will use explicit schemas and casts; it will not rely on permissive inference for production loads. Schema changes will be reviewed before promotion.

## Silver transformations

Silver processing will:

- standardize field names, timestamps, time zones, numeric types, codes, and units;
- validate pickup/drop-off chronology, plausible ranges, required identifiers, and coordinates/zones where applicable;
- normalize TLC service variants into conformed fields while retaining source-specific detail;
- join authoritative taxi-zone reference data;
- map weather observations to appropriate time and geographic grain;
- separate valid, rejected, and reviewable records;
- retain lineage metadata linking outputs to source and run.

## Gold fact and dimension model

The primary fact will be `fact_trip` at one accepted TLC trip record per row. Candidate measures include trip count, passengers, distance, duration, fare components, total amount, and calculated speed where valid.

Planned conformed dimensions include:

- `dim_date` and `dim_time`
- `dim_taxi_zone` with borough and service-zone attributes
- `dim_service_type`
- `dim_rate_code`
- `dim_payment_type`
- `dim_weather` or a weather observation bridge at the selected spatiotemporal grain

Surrogate keys, unknown members, slowly changing behavior, and late-arriving records will be specified per dimension. Gold aggregates may summarize demand and revenue by hour, day, zone, service type, and weather condition. Every table will declare its grain.

## Snowflake loading

Gold data will enter Snowflake through an Azure-compatible, least-privilege loading pattern chosen during integration design. Loads will be batch-aware and idempotent, using staged files and `copy`, streams/tasks, or controlled `merge` operations as justified by the final architecture. Load audit records will reconcile staged, accepted, updated, rejected, and target counts. Credentials will never be embedded in code or dbt profiles committed to Git.

## dbt models

The dbt project will define sources for loaded Gold/landing tables, thin staging models for warehouse normalization, intermediate models for reusable logic, and marts for Power BI. Materializations will be selected by volume and reuse; large models may be incremental with a tested unique key. Schema YAML will document columns, tests, ownership, and freshness expectations. dbt-generated artifacts will remain untracked.

## Data quality

Checks will operate at the earliest useful layer:

- ingestion: file presence, size, format, and expected period;
- Bronze: readable records, metadata completeness, and schema drift;
- Silver: type validity, required fields, ranges, accepted codes, chronology, uniqueness, and quarantine rate;
- Gold: grain uniqueness, referential integrity, measure reconciliation, and dimension coverage;
- Snowflake/dbt: source freshness, not-null, unique, relationship, accepted-value, and business-rule tests.

Threshold breaches will be classified as warning or failure and stored with the run metadata.

## Pipeline audit metadata

An audit model will capture `run_id`, parent run, environment, pipeline/job name, source, dataset, batch period, code/config version, start/end timestamps, status, input/output/rejected counts, bytes processed, watermark, retry number, and sanitized error information. Dataset-level records will make multi-step reconciliation possible.

## Orchestration

ADF will coordinate acquisition, landing, Databricks jobs, Snowflake loading, dbt execution, and quality gates. Pipelines will accept environment and date/batch parameters, expose explicit dependencies, use bounded retries for transient errors, and avoid overlapping writes to the same batch. Manual backfills will use the same parameterized definitions as scheduled runs.

## Monitoring

Operational monitoring will combine ADF run state, Databricks job results, Delta/audit metrics, Snowflake load history, and dbt test results. Alerts will prioritize actionable conditions: failed pipelines, repeated retries, freshness breaches, abnormal row counts, high quarantine rate, or reconciliation failure. Dashboards and runbooks will link symptoms to the relevant `run_id`.

## Error handling

Configuration and contract failures will stop before writes. Transient network or service failures may retry with exponential backoff and a fixed limit. Data errors will be quarantined when partial acceptance is explicitly allowed; systemic schema or quality failures will block promotion. Writes will use atomic or transactional patterns, failed batches will retain diagnostics, and reruns will use idempotent batch semantics.
