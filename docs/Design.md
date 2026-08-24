# UrbanFlow Technical Design

This document describes the implementation and live validation completed through Phase 8. It separates proven behavior from deferred productionization work.

## Source acquisition

`src/ingestion/` acquires official NYC TLC monthly Parquet and taxi-zone CSV data. The client derives one bounded source-period URL, streams content to a `.part` file, and atomically renames it only after success. Existing files are skipped unless `--force` is supplied. Every attempt receives a UUID `run_id` and a JSONL audit record containing the public source URL, dataset, timing, status, byte/record measure, local path, and sanitized failure detail.

The optional NOAA/NCEI client sends its token only in the required request header, paginates bounded observations, and writes a combined raw JSON document. Without `NOAA_API_TOKEN`, weather is recorded as skipped and no request is made. Weather is not an input to the validated analytical model.

## Storage layout and source preservation

The implemented lake layout is:

```text
bronze/tlc/yellow/year=YYYY/month=MM/source.parquet
bronze/reference/taxi_zones/taxi_zone_lookup.csv
bronze/delta/yellow_taxi/
bronze/delta/taxi_zones/
silver/fact_trips/
silver/dim_taxi_zones/
silver/rejected/trips/
silver/rejected/taxi_zones/
gold/fact_trips/
gold/dim_date/
gold/dim_time/
gold/dim_location/
gold/agg_daily_trips/
gold/agg_location_trips/
gold/agg_hourly_trips/
audit/{bronze,silver,gold}_pipeline/
audit/{bronze,silver,gold}_quality/
```

Local runtime data is ignored. The ADLS uploader authenticates with `DefaultAzureCredential`, uploads in bounded chunks to a temporary remote path, verifies the staged size, and atomically renames it. Same-size existing objects are skipped; a size conflict requires explicit `--overwrite`. The immutable raw objects are never rewritten by Bronze, Silver, or Gold processing.

## Delta Lake and partitioning

Bronze, Silver, and Gold outputs use Delta ACID writes. Yellow Taxi Bronze is partitioned by `_urbanflow_source_year` and `_urbanflow_source_month`. Silver/Gold trip facts, rejected trips, and Gold aggregates use `source_year` and `source_month`. A retry overwrites only the exact source period through `replaceWhere`. Taxi zones and the small deterministic dimensions use complete unpartitioned snapshot replacement.

Blind append and a mutable global watermark are not part of the implementation. The explicit source period is the processing, retry, and backfill boundary.

## Schema, metadata, and lineage

Bronze requires the expected source columns, preserves every source field, and adds source file, UTC ingestion time, run ID, and—where applicable—source year/month. Source file lineage uses Unity Catalog's supported `_metadata.file_path` column.

Silver uses explicit Spark types and stable snake-case names. Money uses `decimal(18,2)`; passenger count uses `decimal(10,2)` so unusual source values are not silently truncated. Bronze, Silver, and Gold run identifiers and timestamps are retained through the fact path. Schema fingerprints are recorded in pipeline audits.

## Silver conformance and rejection design

`fact_trips` is one valid standardized TLC record per row. `trip_id` is a SHA-256 fingerprint over all standardized business columns because the source has no stable trip identifier. Within a source period, deterministic ranking keeps the first fingerprint and routes later ranks to rejection.

Trips are rejected for critical timestamp/location failures, drop-off before pickup, negative passenger count, invalid distance, required-money failures, decimal overflow, unknown location references, or duplicates. Taxi zones are rejected for invalid identifiers, missing labels, or duplicate IDs. Rejected records retain ordered `rejection_rules`, a primary rule, a joined reason, rejection timestamp, lineage, and the uncast Bronze record as JSON.

Finite negative monetary values are preserved and marked as financial adjustments. Null passenger counts are retained as unknown. These are warnings rather than reasons to erase a real trip.

## Gold analytical design

The Gold fact retains one row per valid Silver trip and the deterministic `trip_id`. Date keys use `yyyyMMdd`, time keys use `HHmm`, and location keys reuse validated TLC identifiers. `dim_date` spans the complete represented pickup/drop-off range, `dim_time` contains all 1,440 minutes with governed day parts, and `dim_location` is a traceable normalized taxi-zone snapshot.

Duration, speed, fare-per-mile, and tip-percentage calculations are guarded by mathematically valid denominators; invalid cases return null rather than zero, infinity, or NaN. Non-adjustment revenue and financial-adjustment amount remain separate measures without filtering the trip.

Daily and hourly aggregates are attributed to pickup date/time. Location revenue and distance are attributed to pickup location while both pickup and drop-off counts are exposed. All aggregate grains retain the source-period keys and reconcile to the fact.

## Snowflake loading

Phase 7 uses Databricks Serverless's supported Snowflake Spark connector with `sfauthenticator=snowflake_jwt`, `pem_private_key`, name-based column mapping, staging-table safety, and Snowflake-managed internal transfer staging. Connector options are produced by one reusable utility and never include a password, Azure credential, external-stage path, or repository-held key. The private PEM is retrieved at runtime from a configurable Databricks secret scope; secret retrieval fails closed with an actionable message. At the Spark boundary, UrbanFlow normalizes actual or escaped line endings, validates an unencrypted RSA PKCS#8 PEM, reserializes it canonically, and supplies only the whitespace-free base64 payload required by the JVM connector. Encrypted, RSA PKCS#1, empty, malformed, and non-RSA keys fail without exposing key material. The Python control connection independently converts the full PEM to DER PKCS#8 bytes.

The seven Gold tables have explicit Snowflake types matching the Phase 6 contracts. Each run clears only its LANDING table, writes the validated Gold slice/snapshot, and verifies row counts, required keys, duplicate keys, and partition boundaries before ANALYTICS changes. At this transfer boundary, a null `FACT_TRIPS.IS_FINANCIAL_ADJUSTMENT` is normalized to `FALSE` because the Snowflake target is non-nullable; all source columns and non-null flag values remain unchanged. Fact and aggregate plans use `BEGIN`, partition-scoped `DELETE`, `INSERT ... SELECT`, and `COMMIT`; dimensions use the same transaction with deterministic full-snapshot deletion. Exceptions cause rollback. Configuration fails before loading unless LANDING, ANALYTICS, and AUDIT are three distinct schemas, preventing a target replacement from deleting its own landing source. Audits capture run/dataset/period/count/status/timestamps/error/reconciliation/idempotency fields. Reconciliation covers unique fact and dimension keys, both pickup/dropoff date/time/location relationships, source boundaries, and daily/location/hourly totals. The two-pass workflow requires one shared `run_id`, distinct pass numbers, stable counts, and no duplicate keys. Because LANDING tables are shared deterministic objects, the Databricks workflow must enforce a single concurrent Phase 7 run. Final live validation used Databricks job `957309293840081`, run `306537529517430`: all 17 tasks succeeded, both passes were stable, all 40 reconciliation checks passed, and critical integrity failures were zero.

## Phase 8 dbt design

Phase 8 is fully implemented and live-validated under `dbt/`. Its only upstream business-data contract is
the seven committed Phase 7 tables in `URBANFLOW.ANALYTICS`: `FACT_TRIPS`, `DIM_DATE`,
`DIM_TIME`, `DIM_LOCATION`, `AGG_DAILY_TRIPS`, `AGG_LOCATION_TRIPS`, and
`AGG_HOURLY_TRIPS`. LANDING remains a transient transfer boundary and AUDIT remains operational
evidence; neither is a dbt presentation source. Live execution publishes only to `URBANFLOW.DBT_DEV`.

### Implemented layering and lineage

```text
source('urbanflow_analytics', ...)
        -> seven stg_* views
        -> int_trip_enriched (ephemeral)
        -> mart_trip_details (view)

stg_agg_daily + stg_dim_date         -> mart_daily_mobility (view)
stg_agg_hourly + stg_dim_date        -> mart_hourly_mobility (view)
stg_agg_location + stg_dim_location  -> mart_location_mobility (view)
```

Source group `urbanflow_analytics` maps logical aggregate names through `identifier` to the
physical Phase 7 `*_TRIPS` relations. Each staging model explicitly selects every upstream
contract column, exposes lower-case names, and preserves types, keys, grain, measures, null
semantics, and lineage without filters or calculations. All downstream SQL uses `ref()` and
contains explicit projections; no production model hardcodes `URBANFLOW.ANALYTICS` or uses
`SELECT *`.

`int_trip_enriched` resolves date, minute, and location dimensions in pickup and drop-off roles
at one row per trip. This is a justified reusable semantic operation: it centralizes six joins
and consistent role-prefixed naming without recomputing Gold metrics. It is ephemeral so the
lineage is visible and reusable while avoiding another persisted copy of the full fact.

`mart_trip_details` retains one row per validated trip for detailed service, payment, passenger,
duration, distance, financial, pickup/drop-off, and lineage analysis. The daily, hourly, and
location marts preserve the authoritative Phase 7 aggregate grains and measures while adding
conformed calendar or location attributes. This enables BI-friendly labels without duplicating
Databricks aggregation logic. All marts are deterministic views because current requirements do
not justify incremental state; a persisted or incremental design requires live workload evidence,
a tested unique key, and bounded retry/backfill behavior.

### Tests and inherited contracts

Generic source/staging tests preserve Phase 7 key nullability and uniqueness and the six validated
fact relationships. Aggregate staging relationships to date/location dimensions guard the joins
used by marts. Intermediate and mart YAML tests cover trip keys, resolved dimension attributes,
business-key components, relationships, non-null governed fields, and the four authoritative
hourly day parts. Singular tests cover the three Phase 7 aggregate source keys, preservation of
fact row count through the six-way enrichment, all three mart aggregate keys, nonnegative count
measures, and the rule that daily financial-adjustment count cannot exceed trip count.

The dbt layer deliberately does not duplicate Phase 7 load counts, partition boundaries, audit
records, transactional replacement, aggregate-total reconciliation, or two-pass idempotency.
Missing passenger counts, nullable location labels, negative financial adjustments, and other
preserved upstream semantics are not converted into failures. Source freshness remains unset
because Phase 7 defines no authoritative actionable freshness SLA or loaded-at contract.

### Documentation and materialization artifacts

Source/model YAML and the layer READMEs document purpose, grain, keys, important measures,
materialization, ownership, and lineage. Offline and live parsing produce the complete lineage
manifest in disposable directories. Controlled `dbt docs generate` produced and validated the
catalog, manifest, and rendered index outside the repository. Generated `target/`, `logs/`,
`dbt_packages/`, and rendered documentation remain untracked.

### Environment, credentials, and permissions

UrbanFlow uses Python 3.11+ and constrains both `dbt-core` and `dbt-snowflake` to the 1.9 release
line. The local environment uses dbt Core 1.9.11, dbt-snowflake 1.9.4, and
snowflake-connector-python 3.18.1. The 1.9 constraint preserves Phase 7's validated connector
`<4` boundary; dbt reports that this release line is deprecated, so a future upgrade must validate
connector 4.x compatibility before changing it. The dbt project and profile are both named
`urbanflow`. `profiles.yml.example` reads only `DBT_SNOWFLAKE_*`, `DBT_TARGET_SCHEMA`, and
`DBT_THREADS`; a populated `profiles.yml` remains external or ignored.

The user completed the manual Snowflake prerequisites before live validation: the `DBT_DEV`
schema, configured `SECURITYADMIN` role boundary, warehouse/database/source usage, target view
creation, key-pair association, and external environment/profile configuration. The connected
session confirmed `SECURITYADMIN`, `COMPUTE_WH`, `URBANFLOW`, and `DBT_DEV`. Codex did not change
roles or grants, and a read-only query-history check found zero ANALYTICS mutation queries during
the validation window. Phase 8 does not change Phase 7 tables, Azure resources, or Databricks
resources. Any future CI/CD implementation should reuse the same externalized profile contract.

### Live validation evidence

`dbt debug` and parse succeeded with 7 sources, 12 models, and 95 tests. `dbt build` completed
106/106 resources in 30.95 seconds: 11 view models succeeded and 95 tests passed with no warnings,
errors, or skips. The standalone test run also passed 95/95. Snowflake contained exactly seven
staging and four mart views in `DBT_DEV`; the intermediate remained ephemeral. All seven staging
counts and all four mart counts matched Phase 7, and exact daily, hourly, and location measure
mismatch counts were zero. Documentation generation described all 12 models and all 7 sources.

## Data quality strategy

Quality checks execute where the relevant contract first becomes authoritative:

- **Acquisition:** request/file success, final-path publication, and audit status.
- **Bronze:** readable non-empty input, required schema, persisted counts, partitions, and observational source metrics.
- **Silver:** cast validity, required fields, chronology, finite/range rules, deterministic duplicates, taxi-zone relationships, rejection reconciliation, and quarantine rate.
- **Gold:** critical keys, dimension uniqueness, all fact relationships, finite derived metrics, schema retention, and daily/hourly/location aggregate reconciliation.
- **Snowflake:** landing schemas/counts/keys/boundaries, transactional target state, relationships, aggregate totals, audits, and two-pass idempotency.
- **dbt:** nullability, uniqueness, accepted values, relationships, row preservation, mart keys, and focused business rules.

A `WARNING` preserves useful but imperfect real data; a critical failure blocks promotion. Metrics are stored by run and dataset. No test is weakened by rewriting governed upstream data.

## Audit and reconciliation

Acquisition and upload write local JSONL events. Bronze, Silver, and Gold use structured Delta pipeline and quality audit datasets. Snowflake writes load and reconciliation evidence to `URBANFLOW.AUDIT`. Together these records capture run, dataset, source/target, source period, timestamps, counts, status, schema fingerprint where applicable, quality outcome, duration, pass identity, and sanitized errors.

Material boundaries reconcile input, valid, rejected, landing, target, dimension, and aggregate counts. The final Phase 7 run passed 40 reconciliation checks, and Phase 8 independently matched all staging/mart counts and aggregate measures to `ANALYTICS`.

## Idempotency and recovery

Local downloads and uploads use temporary publication and existence checks. Delta facts/aggregates use exact source-period replacement. Small dimensions use deterministic snapshots. Snowflake uses landing validation plus explicit transactions and scoped replacement. dbt relations are deterministic views, with the trip-enrichment model ephemeral.

Two Databricks/Snowflake passes with stable counts were required for completion. Failed Snowflake target work rolls back; failed configuration stops before writes. Backfill or retry must supply the same bounded source period and must not disturb unrelated periods.

## Orchestration and monitoring boundary

There is no deployed central orchestrator, schedule, alerting service, or SLA monitor. Phase workflows were executed in controlled Databricks jobs and through explicit dbt commands. Existing audit tables, test results, and job histories provide validation evidence, but continuous operational monitoring is not claimed.

Azure Data Factory was evaluated during Phase 9 and intentionally deferred. No ADF definition is part of the implemented design. A future orchestrator may coordinate the existing parameterized steps, but it must preserve their ownership boundaries, idempotency, security, and reconciliation behavior.

## Error handling and security

Configuration and critical contract failures stop before target promotion. Network operations use bounded retries where implemented. Data-level failures are quarantined only when partial acceptance is explicitly allowed; errors are sanitized before audit output.

Azure access uses identity-based authentication. Snowflake key-pair material, populated dbt profiles, and secret-like environment values remain outside the repository. `ANALYTICS` is the read-only dbt source and `DBT_DEV` is the separate target. Raw datasets, private keys, populated `.env` files, dbt targets/logs/packages, and rendered artifacts are not versioned.

## Implemented Phase 2 acquisition design

The local package under `src/ingestion/` now establishes the concrete source boundary:

- `config.py` loads TLC, NOAA, and local path settings from environment variables and `.env` without embedding credentials.
- `tlc_client.py` constructs official monthly TLC URLs and streams trip/reference downloads in bounded chunks.
- `weather_client.py` constructs NOAA CDO v2 data requests, sends the token only in the `token` header, follows result metadata with `limit=1000` and increasing offsets, and persists one combined raw JSON document.
- `ingestion_audit.py` appends one JSON Lines record per attempted, completed, failed, or skipped operation.
- `run_ingestion.py` exposes `tlc`, `taxi-zones`, `weather`, and `all` source commands.

The initial development contract is one `yellow_tripdata_YYYY-MM.parquet` object per invocation. The Phase 2 default is `2026-05`; `TLC_YEAR`, `TLC_MONTH`, and `TLC_TAXI_TYPE` control later bounded runs. Trip files land at `data/bronze/tlc/{type}/year=YYYY/month=MM/source.parquet`, while taxi zones use a separate reference path.

Downloads use a deterministic `.part` path in the destination directory. HTTP status is validated before content is accepted; exceptions remove the temporary object, and `os.replace` publishes the final file only after a non-empty download completes. A final file causes a skip unless the caller passes `--force`.

NOAA defaults to the `GHCND` daily dataset and Central Park station `GHCND:USW00094728`, with configurable dates and units. Live weather acquisition is disabled when `NOAA_API_TOKEN` is absent and no NOAA request is made. The token must be obtained manually from NOAA before enabling that source.

The local audit schema contains `run_id`, source, dataset, URL, UTC start/completion timestamps, status, records or bytes, local path, and sanitized error text. Local source artifacts and audit files stay ignored by Git.

## Implemented Phase 3 Azure storage design

`src/azure_storage/` adds a narrow storage boundary without changing Phase 2 acquisition:

- `config.py` accepts only non-secret account, filesystem, and local data-path configuration.
- `client.py` creates `DefaultAzureCredential` and `DataLakeServiceClient` instances, maps local Bronze paths to POSIX ADLS paths, checks remote properties, uploads files or directory contents, and validates final sizes.
- `uploader.py` selects TLC, taxi-zone, weather, or all available files for the configured month and never triggers a source download.
- `audit.py` writes credential-free upload outcomes to `data/audit/azure_upload_audit.jsonl`.

### Upload safety and idempotency

Before writing, the client reads remote file properties. A matching remote size returns `skipped` and transfers zero bytes. A size mismatch fails with an explicit conflict unless the operator deliberately supplies `--overwrite` after review.

New content is written in 4 MiB chunks to a unique temporary file beside the destination. The client flushes and verifies the staged byte count, atomically renames the staging file to the final path, and verifies the published size. A failed staged upload triggers cleanup of its temporary object. Only directories needed by an uploaded file are created.

### Identity and audit

Local authentication uses `DefaultAzureCredential`; during validation it selected the identity already authenticated through Azure CLI. Account keys, SAS tokens, connection strings, client secrets, and passwords are not accepted or logged.

Each source attempt records run ID, source, absolute local path, remote path, storage account, filesystem, UTC timestamps, status, bytes uploaded, and sanitized error text. Missing local weather is an audited skip rather than a failure.

### Integration validation

The May 2026 Yellow Taxi Parquet file and TLC taxi-zone CSV were uploaded to the existing `urbanflow` filesystem and verified at 69,699,174 and 12,331 bytes respectively. A second non-overwrite run detected both files and skipped them with zero transferred bytes. The first integration attempt was audited as failed after a single large SDK request timed out and before any final file was published; the implemented chunked staging design corrected that behavior.

## Implemented Phase 4 Bronze processing design

### Notebook and path contract

- `01_ingest_yellow_taxi.py` reads the configured raw year/month Parquet object with its inferred source schema, verifies required fields and a nonzero count, preserves every source column, and adds `_urbanflow_source_file`, `_urbanflow_ingested_at_utc`, `_urbanflow_run_id`, `_urbanflow_source_year`, and `_urbanflow_source_month`.
- `02_ingest_taxi_zones.py` reads the headered CSV with inferred source types, verifies `LocationID`, `Borough`, `Zone`, and `service_zone`, and adds source-file, ingestion-time, and run metadata.
- `03_bronze_quality.py` reads the processed Yellow Taxi Delta batch and appends report-only metrics.
- `04_validate_phase4.py` reconciles raw and Delta counts, metadata, partition columns, quality output, and two successful Yellow Taxi retry audits.
- `utilities/bronze_common.py`, `audit.py`, and `quality.py` hold shared contracts. Spark imports are lazy so local tests require no PySpark installation.

The configured root is `abfss://urbanflow@urbanflowdata2026.dfs.core.windows.net/`. Raw objects are never write targets. Processed targets are `bronze/delta/yellow_taxi/`, `bronze/delta/taxi_zones/`, `audit/bronze_pipeline/`, and `audit/bronze_quality/`.

### Quality classification and thresholds

Critical ingestion failures stop the notebook and append a `FAILED` pipeline audit when possible: unreadable source/target, zero source rows, missing required columns, Delta write failure, or post-write row-count mismatch. Content checks are report-only warnings with a zero threshold: any null pickup timestamp, null dropoff timestamp, null pickup location, null dropoff location, exact duplicate excess row, dropoff earlier than pickup, negative passenger count, negative fare amount, or negative total amount produces `quality_status=WARNING`. Zero findings across those metrics produces `PASSED`. No quality rule filters, fixes, deduplicates, quarantines, or promotes Bronze records.

Exact duplicates are evaluated across the preserved source columns and counted as excess rows beyond the first copy. Null timestamp/location counts are separate from chronology. The initial Phase 4 invalid-timestamp definition is specifically a non-null dropoff earlier than pickup.

### Audit, schema, and idempotency

`audit/bronze_pipeline/` is append-only Delta with `run_id`, `pipeline_name`, `dataset`, `source_path`, `target_path`, `started_at_utc`, `completed_at_utc`, `status`, `row_count`, `schema_version`, `quality_status`, `error`, and `duration_ms`. Schema version is a SHA-256 fingerprint of Spark schema JSON. `audit/bronze_quality/` stores one row per metric with run, dataset, value, threshold, and outcome.

Yellow Taxi is partitioned only by `_urbanflow_source_year` and `_urbanflow_source_month`. Its writer uses Delta `overwrite` with an exact `replaceWhere` predicate, so a retry replaces one batch while retaining other periods and cannot append retry copies. Taxi zones remain unpartitioned and use complete snapshot overwrite. Both writers verify persisted cardinality.

### Databricks workspace configuration

The implemented Unity Catalog setup uses storage credential `urbanflow_adls_managed_identity` with the system-assigned managed identity at `/subscriptions/c6fc33b6-e352-46d2-8a92-81b8f1cd3e15/resourceGroups/rg-urbanflow/providers/Microsoft.Databricks/accessConnectors/ac-urbanflow`, plus external location `urbanflow_adls_root` at the filesystem root. An equivalent manual setup is: in Catalog, open **Connect > Credentials**, create a storage credential using **Azure managed identity** and that Access Connector resource ID; then open **External data > External locations**, create `urbanflow_adls_root` with the ABFSS root and the new credential. The Azure-side connector and Storage Blob Data Contributor assignment must already exist; do not create keys or secrets.

### Final Phase 4 integration validation

Unity Catalog Serverless validation completed successfully after source lineage was changed to the supported `_metadata.file_path` field. Yellow Taxi reconciled at 4,090,836 rows between raw Parquet and Delta after the second ingestion; taxi zones reconciled at 265 rows between raw CSV and Delta. `DESCRIBE DETAIL` confirmed the two intended Yellow Taxi partition columns and no taxi-zone partitions. Two successful Yellow Taxi audit records were checked.

Quality completed with `WARNING`, not failure: negative fare amount count was 14,231 and negative total amount count was 14,877. Duplicate, invalid-timestamp, negative-passenger, and all four requested null counts were zero. The result validates the Phase 4 rule that real-source anomalies remain visible while Bronze records remain unchanged.

## Implemented Phase 5 Silver transformation design

### Explicit schema and standardization

`fact_trips` renames TLC fields to stable snake_case names. Timestamps use Spark `timestamp`; vendor, rate, payment, and location IDs use `integer`; trip distance uses `double`; passenger count uses `decimal(10,2)` to avoid silently truncating unusual provider values. Fare components use `decimal(18,2)`, which provides cent precision and ample range without binary floating-point error. String flags and taxi-zone labels are trimmed, with the store-and-forward flag uppercased.

Lineage columns are `source_file`, `ingested_at_utc`, `source_year`, `source_month`, and `bronze_run_id`. Silver adds `trip_id`, `is_financial_adjustment`, `silver_run_id`, and `silver_processed_at_utc`.

### Classification and quarantine

Trips are rejected for missing pickup/dropoff timestamps or locations, dropoff before pickup, negative passenger count, missing/non-finite/negative distance, missing required fare/total, monetary values that cannot fit `decimal(18,2)`, unknown pickup/dropoff zone IDs, or deterministic duplicates. A row can fail multiple rules; quarantine stores `rejection_rules` as an ordered array, `rejection_rule` as the primary rule, and `rejection_reason` as a pipe-delimited summary. `bronze_record_json` preserves original uncast values for diagnosis.

Taxi zones are rejected for null IDs, missing borough/zone names, or duplicate location IDs. Valid location IDs come from this reference contract, not a hardcoded range.

### Monetary and passenger-count policy

The official TLC dictionary defines payment codes for no-charge, disputed, and voided trips, while TLC notes that provider-submitted records are published without guaranteed accuracy. A negative amount alone is therefore not sufficient evidence that the trip should disappear from the analytical fact. Finite values that fit `decimal(18,2)` are retained and set `is_financial_adjustment=true`; counts remain Silver quality warnings. Null required fare/total, non-finite values, and decimal overflow are quarantined.

The first live quality run showed that 955,371 rows—23.3539% of the batch—failed only because passenger count was null. Passenger count is provider/driver-reported and does not determine whether a trip occurred, so null is retained as analytically unknown; negative passenger counts remain invalid. This policy preserved otherwise valid trips while making missingness visible in quality metrics.

Sources: [TLC Yellow Taxi data dictionary](https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf) and [TLC Trip Record User Guide](https://www.nyc.gov/assets/tlc/downloads/pdf/trip_record_user_guide.pdf).

### Deduplication, incrementality, and idempotency

The source has no stable trip identifier. `trip_id` is a SHA-256 fingerprint over all standardized business columns. Within each source year/month, rows are ranked deterministically by source lineage; rank greater than one is quarantined as `DUPLICATE_TRIP`. The May 2026 batch contained zero duplicates.

Fact and rejected-trip writes use Delta `overwrite` with an exact `replaceWhere` predicate on `source_year` and `source_month`, retaining all other months. Taxi-zone valid/rejected snapshots use full unpartitioned overwrite. A second live run produced the same 4,090,836 valid and zero rejected trip counts, and final validation checked two matching successful fact audits.

### Silver quality thresholds and audit

Quality is `FAILED` when the source is empty, no valid rows exist, valid plus rejected does not reconcile to source, or rejection rate exceeds 20%. It is `WARNING` when reconciliation passes but observed missing passenger counts, negative money, rejected rows, duplicates, timestamp failures, or location failures are nonzero. Otherwise it is `PASS`.

Pipeline audits store run/pipeline/dataset, source and target paths, UTC timestamps, source/valid/rejected counts, quality status, schema fingerprint, duration, and sanitized error. Quality audit stores one structured row per metric and run. No credential material is recorded.

### Final Serverless validation

Job `713366891015169`, run `841707541463751`, completed all eight ordered tasks successfully with no user cluster configuration. Final counts were 4,090,836 Bronze trips, 4,090,836 valid Silver trips, zero rejected trips, 265 Bronze zones, 265 valid zones, and zero rejected zones. Referential failures and duplicate valid trip IDs were zero. Final quality was `WARNING` for 955,371 null passenger counts, 14,231 negative fares, and 14,877 negative totals; every requested timestamp/location anomaly, negative passenger/distance count, duplicate count, and rejection count was zero.

## Implemented Phase 6 Gold analytical design

### Grain, schemas, and derived measures

`gold/fact_trips` retains the Silver grain of one valid taxi trip and the original deterministic `trip_id`. It preserves source file, source year/month, ingestion timestamp, Bronze run, Silver run/timestamp, and adds Gold run/timestamp. Pickup/dropoff date keys use `yyyyMMdd`; time keys use `HHmm`; location keys remain validated TLC integers.

Money retains the Silver `decimal(18,2)` contract. Trip duration, average speed, fare per mile, and tip percentage are doubles calculated only when their denominators are mathematically valid. Zero-duration speed, zero-distance fare-per-mile, and nonpositive-fare tip percentage are null. No calculation creates infinity or NaN. `non_adjustment_revenue` and `financial_adjustment_amount` make the two revenue views explicit without filtering a trip.

### Dimensions and aggregates

`dim_date` is generated from the minimum through maximum pickup/dropoff date represented by the complete Silver fact, so it grows reproducibly as Silver history grows. The current source range yields 6,363 continuous calendar rows. `dim_time` contains exactly 1,440 unique minute keys and Overnight, Morning, Afternoon, and Evening categories. `dim_location` is a traceable normalized snapshot of all 265 Silver taxi zones.

Daily and hourly revenue is attributed to pickup date/time. Location revenue, distance, and averages are attributed to pickup location, while both pickup and dropoff trip counts are exposed. Aggregate rows retain source year/month so one bounded source batch can be replaced exactly. The live batch produced 35 daily, 265 location, and 748 hourly rows; their trip-count perspectives all reconcile to 4,090,836.

### Idempotency, quality, and audit

Fact and aggregate writers use Delta overwrite with exact `replaceWhere` predicates and year/month partitions. Dimensions use full overwrite because they are small deterministic reference outputs. Repeated workflow passes retained 4,090,836 unique fact rows and stable aggregate counts.

Gold quality fails for empty facts, duplicate/null critical keys, duplicate dimension keys, date/time/location referential failures, impossible negative duration/distance, non-finite derived metrics, schema loss, or aggregate reconciliation failure. Missing passenger counts and financial-adjustment trips are warnings because Silver deliberately preserved them. Final quality was `WARNING`: 955,371 passenger counts were unknown and 14,953 trips contained at least one negative monetary component; every critical metric was zero.

`audit/gold_pipeline/` records run, pipeline, dataset, source/target, UTC timestamps, status, row count, schema fingerprint, quality status, duration, and sanitized error. `audit/gold_quality/` records one typed row per metric and run. No credential material is recorded.

### Final Serverless validation

Databricks Serverless job `631815480020120` completed the fourteen-task two-pass workflow successfully in runs `191548502476871` and `150700319211970`. Final validation checked two successful fact audits, exact Silver/Gold cardinality, schemas, partitions, unique keys, all dimension relationships, aggregate reconciliation, and persisted quality metrics. Serverless terminated automatically and no Azure infrastructure or secret mechanism changed.
