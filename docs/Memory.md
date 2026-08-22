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
