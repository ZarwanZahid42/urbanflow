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
