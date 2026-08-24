# UrbanFlow Project Memory

This file records durable architectural and implementation decisions. Add a dated entry when a decision changes system behavior, interfaces, security, cost, or operations. Keep proposed ideas separate from accepted decisions.

## Current decisions

| Decision | Status | Rationale |
|---|---|---|
| Project name: UrbanFlow | Accepted | Establishes a consistent project identity. |
| Azure-only cloud strategy | Accepted | Microsoft Azure is the required cloud platform; AWS is excluded unless scope changes explicitly. |
| NYC TLC as the primary real data source | Accepted | Provides public, real, high-volume NYC mobility records. |
| Weather data as the planned secondary source | Accepted | Enables analysis of external conditions against mobility; the provider remains to be selected. |
| Medallion Architecture | Accepted | Separates raw fidelity, validated data, and analytics-ready models. |
| ADLS Gen2 as the data lake | Accepted | Provides the durable Azure-native landing and lake storage layer. |
| Databricks as the processing engine | Accepted | Supports scalable PySpark and Delta Lake transformations. |
| Snowflake as the analytical warehouse | Accepted | Hosts curated analytical models for downstream consumption. |
| dbt for warehouse transformations and testing | Accepted | Provides modular SQL, tests, lineage, and documentation. |
| Azure Data Factory for orchestration | Accepted | Coordinates ingestion and downstream pipeline dependencies in Azure. |

## Decision log

### 2026-08-22 — Foundation scope

- **Decision:** Initialize only repository structure, documentation, and minimal local configuration.
- **Reason:** Establish a clear, interview-focused foundation before implementing or provisioning components.
- **Consequences:** No pipelines, fake datasets, cloud resources, external integrations, or large platform dependencies are included in Phase 1.

### 2026-08-22 — Phase 2 real-data acquisition

- **Decision:** Use the official NYC TLC predictable monthly Parquet pattern, with May 2026 Yellow Taxi data as the single local development slice.
- **Reason:** May 2026 was the latest published Yellow Taxi month on the official TLC page at implementation time and provides a bounded real dataset.
- **Alternatives considered:** Kaggle and synthetic datasets were rejected; multi-month downloads were deferred to avoid unnecessary local volume.
- **Consequences:** TLC year, month, and taxi type are environment-configurable; downloads are streamed, retry-safe, and locally audited.

- **Decision:** Use NOAA/NCEI Climate Data Online API v2 with optional token-gated acquisition, `GHCND` daily observations, and Central Park station `GHCND:USW00094728` as defaults.
- **Reason:** NOAA is an authoritative real weather source and its API supports station/date filtering and pagination.
- **Consequences:** Live weather is skipped without `NOAA_API_TOKEN`; obtaining the token is a manual prerequisite. No NOAA call is made during Phase 2 validation without credentials.

### 2026-08-22 — Phase 3 existing ADLS Gen2 integration

- **Decision:** Use the manually created `urbanflowdata2026` HNS-enabled storage account and `urbanflow` filesystem in Central India as UrbanFlow's cloud data lake.
- **Reason:** The Azure for Students infrastructure already exists in `rg-urbanflow`; application provisioning would be unnecessary and outside the authorized scope.
- **Alternatives considered:** Creating resources from Python or infrastructure-as-code was explicitly excluded from Phase 3.
- **Consequences:** Application code uploads data only. Resource creation, configuration, access assignment, and Azure CLI sign-in remain manual administrative responsibilities.

- **Decision:** Authenticate the Python application with `DefaultAzureCredential` and the current Microsoft Entra/Azure CLI identity.
- **Reason:** Identity-based access avoids storage keys, SAS tokens, connection strings, passwords, and client secrets while supporting local development and later managed identity hosting.
- **Consequences:** The active identity must already have appropriate data-plane access, and local sessions may require `az login --use-device-code` again when authentication expires.

- **Decision:** Preserve local Bronze-relative paths in ADLS and upload only the configured month plus reference/available weather files.
- **Reason:** This retains Medallion partition semantics and prevents accidental multi-month uploads.
- **Consequences:** May 2026 TLC data maps to `bronze/tlc/yellow/year=2026/month=05/source.parquet`; taxi zones map to `bronze/reference/taxi_zones/taxi_zone_lookup.csv`.

- **Decision:** Use remote existence/size checks and staged 4 MiB chunk uploads followed by atomic rename.
- **Reason:** Matching files should not be transferred repeatedly, and slow links made one large SDK request unreliable during integration validation.
- **Consequences:** Same-size destinations are skipped; mismatched destinations require explicit overwrite review; failed staging files are cleaned up; upload outcomes are auditable locally.

## Entry template

### YYYY-MM-DD — Decision title

- **Decision:** What was decided.
- **Reason:** Why it was selected.
- **Alternatives considered:** Relevant alternatives, if any.
- **Consequences:** Implementation and operational effects.

### 2026-08-23 — Phase 4 Databricks Bronze processing

- **Decision:** Keep landed TLC Parquet/CSV objects immutable and write separate Delta datasets under `bronze/delta/`.
- **Reason:** Raw recovery fidelity and replayability must remain independent from optimized Bronze persistence.
- **Consequences:** Notebooks read only the two existing raw paths. Yellow Taxi writes are partition-scoped replacements by source year/month; taxi zones are an unpartitioned replaceable snapshot.

- **Decision:** Use Unity Catalog storage credential `urbanflow_adls_managed_identity` and external location `urbanflow_adls_root`, backed by the existing `ac-urbanflow` system-assigned managed identity.
- **Reason:** Managed identity gives Databricks least-secret access to the existing ADLS filesystem.
- **Alternatives considered:** Keys, SAS tokens, connection strings, passwords, service principals, and client secrets were rejected.
- **Consequences:** The Access Connector and RBAC remain Azure-managed prerequisites; the Databricks workspace stores no secret credential material.

- **Decision:** Treat Bronze content anomalies as zero-threshold warnings and reserve failures for unreadable/empty input, required-schema loss, or persistence/reconciliation failures.
- **Reason:** Bronze must expose source quality without silently applying Silver business transformations.
- **Consequences:** Nulls, duplicates, invalid chronology, and negative values are stored as metrics and never remove records.

- **Decision:** Keep PySpark out of the local dependency set and isolate pure contracts from Spark execution.
- **Reason:** Spark/Delta are supplied by Databricks; local tests should stay fast and deterministic.
- **Consequences:** The 35-test local suite validates paths, audit shape, schema fingerprints, Unity Catalog source metadata, quality classification, and writer semantics with small mocks.

- **Final validation:** Unity Catalog Serverless compute successfully ran Yellow Taxi ingestion twice, taxi-zone ingestion, Bronze quality, and final reconciliation. Yellow Taxi raw and Delta counts both equal 4,090,836; taxi-zone raw and Delta counts both equal 265. Yellow Taxi partitions are `_urbanflow_source_year` and `_urbanflow_source_month`; taxi zones are unpartitioned. Two successful Yellow Taxi ingestion audits confirm idempotent retry cardinality.
- **Final quality result:** `WARNING` is accepted as an observational Bronze outcome. Negative fare count is 14,231 and negative total count is 14,877. Duplicate, invalid-timestamp, negative-passenger, null pickup/dropoff timestamp, and null pickup/dropoff location counts are all zero. No records were removed or corrected.
- **Serverless compatibility:** Source lineage uses the Unity Catalog-supported `_metadata.file_path` field rather than `input_file_name()`. This preserves source-file metadata without changing raw data, paths, Delta layout, audit behavior, or identity configuration.

### 2026-08-23 — Phase 5 Silver transformation

- **Decision:** Use explicit Spark types and snake_case contracts for `fact_trips` and `dim_taxi_zones`; use `decimal(18,2)` for money and `decimal(10,2)` for passenger count.
- **Reason:** Silver must be analytics-ready without binary money error or silent truncation of unusual submitted values.
- **Consequences:** Schema fingerprints and live validation confirm the intended types; invalid casts can be diagnosed from quarantine `bronze_record_json`.

- **Decision:** Preserve finite negative amounts as financial adjustments rather than reject them solely by sign.
- **Reason:** TLC payment semantics include no-charge, dispute, and voided records, and provider-submitted data is not guaranteed accurate. Sign alone does not prove a record should be discarded.
- **Consequences:** `is_financial_adjustment` identifies these rows; 14,231 negative fares and 14,877 negative totals remain visible and produce quality warnings.

- **Decision:** Treat null passenger count as unknown, not rejected; continue rejecting negative passenger counts.
- **Reason:** The first live run showed 955,371 otherwise-valid rows (23.3539%) failed only the provisional null-passenger rule. Passenger count is provider/driver-reported and is not required to prove a trip occurred.
- **Alternatives considered:** Quarantining all 955,371 rows was rejected because it would remove nearly a quarter of valid trips without a defensible trip-validity basis.
- **Consequences:** All 4,090,836 trips are retained; missing passenger count is an explicit quality warning.

- **Decision:** Generate deterministic trip IDs from a SHA-256 fingerprint of all standardized business fields and quarantine later ranks within a source month.
- **Reason:** TLC Yellow Taxi data has no stable trip identifier, and bare `dropDuplicates()` would be undocumented and nondeterministic.
- **Consequences:** Reruns replace the same year/month partition, zero duplicate valid trip IDs were observed, and two successful fact audits reconcile identically.

- **Final validation:** Databricks Serverless job `713366891015169`, run `841707541463751`, succeeded with no user cluster. Trips reconciled 4,090,836 source = 4,090,836 valid + 0 rejected; zones reconciled 265 = 265 + 0. Referential failures and duplicate valid trip IDs were zero. Final quality was `WARNING`, audits and schemas were verified, the second run was stable, and 48 local tests passed.
- **Security/operations:** Existing managed-identity Unity Catalog access was reused. No Azure infrastructure or secret material changed. Serverless compute terminated automatically; the saved validation job is stopped.

### 2026-08-23 — Phase 6 Gold analytical layer

- **Decision:** Preserve the one-valid-Silver-trip grain and deterministic `trip_id` in Gold.
- **Reason:** Gold should add analytical structure without silently changing the validated trip population.
- **Consequences:** Silver and Gold both contain 4,090,836 rows; duplicate Gold trip IDs and critical null keys are zero.

- **Decision:** Use `yyyyMMdd` date keys, `HHmm` minute keys, and TLC location IDs as conformed dimension keys.
- **Reason:** These keys are deterministic, human-auditable, stable for downstream Snowflake/BI use, and require no generated-key state.
- **Consequences:** `dim_date` covers the complete represented range with 6,363 rows, `dim_time` has 1,440 rows, and `dim_location` has 265 rows. All fact relationships validate.

- **Decision:** Guard all derived ratios and preserve financial adjustments explicitly.
- **Reason:** Zero denominators must not produce infinity/NaN, and negative monetary records retained by Silver must remain analytically visible.
- **Consequences:** Invalid derived metrics are zero. Gold exposes non-adjustment revenue and financial-adjustment amount separately; 14,953 adjustment trips produce a warning rather than data loss.

- **Decision:** Replace facts and aggregates by source year/month and rebuild small dimensions as complete deterministic snapshots.
- **Reason:** Monthly TLC batches are the established recovery boundary, while tiny reference dimensions are safer and simpler to replace fully.
- **Alternatives considered:** Blind append was rejected because retries would duplicate facts and aggregates; overly granular date partitioning was rejected.
- **Consequences:** Two complete Serverless passes and later verification retained stable table counts and at least two matching successful fact audits.

- **Final validation:** Job `631815480020120`, runs `191548502476871` and `150700319211970`, completed all fourteen ordered Serverless tasks. Facts reconcile 4,090,836 to Silver; dimension counts are 6,363/1,440/265; aggregation counts are 35/265/748; every aggregation perspective reconciles to 4,090,836; critical quality metrics are zero; quality is `WARNING` for 955,371 unknown passenger counts and 14,953 adjustment trips. The local suite passes 62 tests.
- **Security/operations:** Existing Unity Catalog managed-identity access was reused. No Azure infrastructure or secret material changed, and Serverless compute terminated automatically.


### 2026-08-23 — Phase 7 Snowflake integration design and manual boundary

- **Decision:** Use the Databricks Serverless Snowflake Spark connector and Snowflake-managed internal transfer staging from Gold Delta to `URBANFLOW.LANDING`.
- **Reason:** This is the supported direct transfer path and avoids a second Azure data-access plane.
- **Alternatives considered:** An ADLS external stage, Azure storage integration, SAS, storage keys, service principals, client secrets, and password authentication were rejected as unnecessary and outside the security/infra scope.
- **Consequences:** Data transfer options are isolated and use `snowflake_jwt`; no Azure infrastructure changes are required. The private PEM stays outside the repository and is retrieved only from a configurable Databricks secret reference at runtime.

- **Decision:** Validate in LANDING, then use explicit Snowflake DML transactions for target replacement.
- **Reason:** LANDING isolates partial connector failures, while `BEGIN` / scoped `DELETE` / `INSERT` / `COMMIT` prevents partially replaced ANALYTICS data.
- **Consequences:** Facts and aggregates replace only the configured source-year/month slice; dimensions replace complete validated snapshots. Blind append is not permitted. Uniqueness, null keys, boundaries, row counts, both pickup/dropoff dimension relationships, and aggregate trip totals are checked before completion.

- **Decision:** Require two live passes with one shared `run_id` and distinct `idempotency_pass` values.
- **Reason:** Equal post-load counts alone do not prove which executions were compared; explicit pass identity makes the evidence auditable.
- **Consequences:** The final notebook requires two successful stable target counts for all seven datasets and rejects duplicate keys. Current Phase 6 counts are validation expectations only, never permanent load constants.

- **Bootstrap state:** Database `URBANFLOW`; schemas `LANDING`, `ANALYTICS`, and `AUDIT`; warehouse `URBANFLOW_LOAD_WH`; loader/reader roles; and RSA-enabled service user `URBANFLOW_DATABRICKS_SVC` were created manually before local implementation. Repository code did not create or modify them.
- **Manual boundary:** The analytical/landing/audit table DDL and Databricks-backed scope `urbanflow-snowflake` were created/populated manually. The existing private-key file was streamed directly from its out-of-repository path into secret `snowflake_private_key`; its contents did not enter chat, source, notebooks, logs, or Git.
- **Completion status:** Superseded by the final 2026-08-24 live validation below.

### 2026-08-24 — Spark connector private-key serialization

- **Live evidence:** Databricks job `957309293840081`, run `841801582124941`, completed Snowflake DDL bootstrap and Python control-connection validation, then failed in `landing_first` before data transfer with `Input PEM private key is invalid` from the Spark connector's PKCS#8 parser.
- **Root cause:** The Python connector accepts UrbanFlow's full PEM because the control path parses it and emits DER PKCS#8 bytes. The Spark connector base64-decodes `pem_private_key` directly into a `PKCS8EncodedKeySpec`, so passing the PEM envelope and line breaks is invalid.
- **Decision:** Normalize actual and escaped newlines, validate an unencrypted RSA PKCS#8 PEM, canonically reserialize it, strip the PEM envelope and all payload whitespace, and pass only that payload to the Spark connector. Reject empty, encrypted, RSA PKCS#1, malformed, and non-RSA inputs without including key material in errors or output.
- **Verification:** Synthetic-key authentication tests isolated the serialization behavior. This intermediate failure is superseded by the final successful validation below.

### 2026-08-24 — Phase 7 final live validation

- **Landing failure root cause:** Silver/Gold `is_financial_adjustment` used nullable Spark comparisons. If no known monetary value was negative but at least one optional monetary input was null, Spark three-valued boolean logic produced null. The live Gold fact contained 955,293 such rows, while Snowflake `FACT_TRIPS.IS_FINANCIAL_ADJUSTMENT` is non-nullable.
- **Landing fix:** At the Snowflake landing boundary only, normalize null `is_financial_adjustment` to `FALSE`. Preserve all source columns and every non-null boolean value. A local regression test verifies that no other column receives a default.
- **Transactional failure root cause:** The non-sensitive Databricks `snowflake_schema` setting resolved ANALYTICS to LANDING. The fact replacement therefore deleted the landing source before its `INSERT ... SELECT`, yielding zero inserted rows.
- **Transactional fix:** Correct `snowflake_schema` to `ANALYTICS` and fail closed unless landing, analytics, and audit schema names are distinct. The stored private key and all credentials were not read, printed, or changed.
- **Final evidence:** Databricks job `957309293840081`, run `306537529517430`, completed all 17 tasks successfully. Both landing passes and `idempotency_final` passed. Landing and target counts matched for 4,090,836 facts, 6,363 dates, 1,440 minutes, 265 locations, 35 daily aggregates, 265 location aggregates, and 748 hourly aggregates.
- **Integrity evidence:** Duplicate-key, fact foreign-key, boundary, and aggregate-total failures were zero. All 40 reconciliation checks passed; the run recorded 14 successful loads and zero failed loads. Every dataset had two passes and one distinct target count.
- **Verification and commit:** The complete local suite passed 104 tests, Python compilation and repository diff checks passed, and no diagnostic or secret artifacts were committed. Phase 7 was committed as `ea0f67f feat: complete phase 7 snowflake validation`.

### 2026-08-24 — Phase 8 dbt planning boundary

- **Status at planning time:** This step added planning and a compatible dependency constraint before the local dbt project existed.
- **Decision:** Treat the seven committed `URBANFLOW.ANALYTICS` tables as dbt sources and keep Phase 7 LANDING, ANALYTICS replacement, AUDIT, reconciliation, and idempotency behavior upstream and unchanged.
- **Reason:** Databricks owns Bronze/Silver/Gold data engineering and Phase 7 owns governed warehouse loading. dbt should add modular warehouse presentation, testing, lineage, and consumer-facing marts without duplicating or weakening validated transformations.
- **Planned layering:** Explicit ANALYTICS `source()` declarations feed thin staging models; `ref()` dependencies feed optional reusable intermediate logic and marts with declared business grains. Views are the expected staging default; any incremental mart must have a tested unique key and bounded retry/backfill behavior.
- **Planned validation:** Add schema, not-null, unique, accepted-value, relationship, meaningful freshness, business-rule, and reconciliation tests. Preserve documented null-passenger and financial-adjustment semantics. Generate dbt docs/lineage while leaving generated artifacts untracked.
- **Dependency decision:** Add `dbt-core>=1.9,<1.10` and `dbt-snowflake>=1.9.4,<1.10` to `requirements.txt`. Version 1.9.4 supports Python 3.11 and requires `snowflake-connector-python>=3.13.1,<4`, which is compatible with UrbanFlow's validated `>=3.16,<4` connector range. Newer adapter lines require connector 4.x and would conflict with the committed Phase 7 dependency contract until that upgrade is validated.
- **Security/manual boundary:** Keep populated `profiles.yml`, account identifiers, passwords, tokens, and private keys outside Git and resolve configuration through environment variables or an approved secret mechanism. Database/schema access, role grants, warehouse usage, authentication, target-schema selection, and secure local/CI configuration may require explicit manual Snowflake work. No external resource or credential changed during planning.

### 2026-08-24 — Phase 8 dbt Core initialization

- **Status:** Local initialization is complete under `dbt/`; production sources, models, tests, packages, documentation generation, and live Snowflake validation remain future Phase 8 work.
- **Project contract:** The dbt project and profile are named `urbanflow`. The tracked `profiles.yml.example` resolves only external `DBT_SNOWFLAKE_*`, `DBT_TARGET_SCHEMA`, and `DBT_THREADS` values. A populated `profiles.yml`, generated `target/`, logs, and downloaded packages remain ignored.
- **Structure:** The scaffold contains `dbt_project.yml`, the safe profile example and setup README, model placeholders for staging/intermediate/marts, and a singular-test placeholder. It intentionally contains no source/model SQL or schema YAML.
- **Toolchain:** The project `.venv` uses Python 3.12.13, dbt Core 1.9.11, dbt-snowflake 1.9.4, and snowflake-connector-python 3.18.1. dbt 1.9 is deprecated but remains pinned because it preserves the validated Phase 7 connector `<4` boundary; upgrading requires separate connector 4.x validation.
- **Validation:** `dbt parse` succeeds offline with a temporary placeholder-only profile and no Snowflake connection. The complete local pytest suite passes 104 tests, dependency checks pass, and the repository diff check is clean.
- **Security/manual boundary:** No Snowflake or Azure resource, grant, profile, credential, private key, or secret changed. A user must explicitly select the target schema and least-privilege role, grant required source-read/target-write/warehouse access, and provide approved external environment values before any live dbt command.

### 2026-08-24 — Phase 8 source contract and local staging layer

- **Status:** The seven Phase 7 ANALYTICS sources and seven source-backed staging views are implemented locally and parse without a Snowflake connection. Intermediate models, marts, docs generation, and live execution remain out of scope.
- **Source naming:** dbt source group `urbanflow_analytics` exposes logical names `fact_trips`, `dim_date`, `dim_time`, `dim_location`, `agg_daily`, `agg_location`, and `agg_hourly`. The aggregate names map through `identifier` to authoritative physical relations `AGG_DAILY_TRIPS`, `AGG_LOCATION_TRIPS`, and `AGG_HOURLY_TRIPS`.
- **Staging decision:** `stg_fact_trips`, the three dimension views, and the three aggregate views explicitly project every contracted column to lower-case names. They preserve Phase 7 types, keys, grains, measures, lineage, and null semantics; no filter, aggregation, conformance, or business calculation is added.
- **Test boundary:** Generic source/staging tests cover guaranteed key nullability/uniqueness and the six validated fact-to-dimension relationships. Three singular source tests cover aggregate composite-key uniqueness. Phase 7 remains responsible for loading, partition boundaries, reconciliation, audits, and idempotency.
- **Freshness decision:** No freshness metadata is declared because the repository defines neither an authoritative warehouse freshness SLA nor an actionable loaded-at contract.
- **Security/manual boundary:** Offline parsing uses disposable placeholders and temporary output only. No credential, profile, grant, table, schema, warehouse, Databricks object, or Azure resource changed. Live validation still requires explicitly approved Snowflake access and target-schema configuration.
### 2026-08-24 — Phase 8 complete local transformation layer

- **Status:** The complete source-to-mart dbt implementation is ready for controlled live Snowflake validation. Live `dbt debug`, build/test execution, catalog generation, and reconciliation have not occurred.
- **Architecture decision:** Keep all seven Phase 7 ANALYTICS contracts as immutable sources and all seven staging models as views. Add only one intermediate model, `int_trip_enriched`, because resolving date, minute, and location dimensions in both pickup and drop-off roles is a reusable six-join semantic operation. Materialize it as `ephemeral` to avoid persisting another 4.09-million-row fact copy.
- **Mart decision:** Publish `mart_trip_details` at trip grain plus `mart_daily_mobility`, `mart_hourly_mobility`, and `mart_location_mobility` at their authoritative Phase 7 aggregate grains. Materialize all four as deterministic views. The aggregate marts add conformed labels but reuse upstream measures; no Gold aggregation, adjustment classification, audit, reconciliation, partition, or idempotency logic is duplicated.
- **Test decision:** Preserve all existing source/staging tests, add aggregate-to-dimension relationships, model keys and governed day-part tests, intermediate row preservation, three mart composite keys, and focused count rules. Do not invent freshness, weather, service-type, or hardcoded live-row-count tests without an authoritative contract.
- **Offline evidence:** `dbt parse` succeeds with disposable placeholder identifiers and no Snowflake connection. The manifest contains 7 sources, 12 models, and 95 tests (85 generic and 10 singular) with the expected `source()`/`ref()` lineage and materializations. `dbt compile --no-introspect` still requires the Snowflake adapter to read its configured key path; the offline attempt stopped on a nonexistent placeholder before any connection and was not bypassed with real credentials. The complete Python suite passes 114 tests; 85 Python files compile with bytecode redirected outside the repository; `pip check` reports no broken requirements.
- **Security/artifact evidence:** No populated `dbt/profiles.yml`, private-key filename, `.env`, generated dbt directory, bytecode, or large artifact is a version-control candidate. Content scans find no token/key patterns or populated secret assignments in new files; existing PEM-header literals are limited to the fail-closed validator and synthetic test fixture. No credential/private-key content or cloud resource was accessed or changed.
- **Manual boundary:** A human must select the approved dbt target schema and least-privilege role, approve database/warehouse/source-read/target-schema permissions, confirm key-pair authentication, and provide identifiers plus the private-key path externally before live validation. Exact grants are intentionally not invented.
- **Git state:** The complete Phase 8 change set remains unstaged and uncommitted for review.

### 2026-08-24 — Phase 8 final live validation

- **Status:** Phase 8 is complete. The controlled Snowflake workflow passed from the current repository state without starting Phase 9.
- **Configuration boundary:** All eight required `DBT_*` variables were visible to the process. The external profile and private-key path existed outside the repository, no repository-local `profiles.yml` existed, and the target was exactly `URBANFLOW.DBT_DEV`, distinct from ANALYTICS. No credential value or private-key content was printed or read.
- **Connection evidence:** `dbt debug` succeeded with dbt Core 1.9.11 and dbt-snowflake 1.9.4. A live query confirmed role `SECURITYADMIN`, warehouse `COMPUTE_WH`, database `URBANFLOW`, and schema `DBT_DEV`; Codex did not switch roles or modify grants.
- **Build and test evidence:** Parse found 7 sources, 12 models, and 95 tests. `dbt build` completed all 106 resources in 30.95 seconds: 11 view models succeeded and 95 tests passed with zero warnings, errors, or skips. The requested standalone `dbt test` also passed 95/95.
- **Live relation evidence:** `DBT_DEV` contains exactly 11 views: seven `STG_*` views and four `MART_*` views. `int_trip_enriched` remains ephemeral and did not create a relation. `MART_TRIP_DETAILS` contains 4,090,836 rows; the daily/hourly/location marts contain 35/748/265 rows.
- **Phase 7 reconciliation:** All seven staging counts and all four mart counts matched their governed ANALYTICS sources with zero differences. Exact daily, hourly, and location measure mismatch counts were zero. Phase 7 counts remain 4,090,836 facts, 6,363 dates, 1,440 minutes, 265 locations, and 35/265/748 daily/location/hourly aggregates.
- **Documentation and safety:** `dbt docs generate` produced `manifest.json`, `catalog.json`, and `index.html` outside the repository; all 12 models and 7 sources are described. A live two-hour query-history audit found zero ANALYTICS mutation queries. No Azure infrastructure, Snowflake privilege, role, upstream relation, secret, repository-local profile, or generated dbt artifact was changed.
