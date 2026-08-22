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

## Entry template

### YYYY-MM-DD — Decision title

- **Decision:** What was decided.
- **Reason:** Why it was selected.
- **Alternatives considered:** Relevant alternatives, if any.
- **Consequences:** Implementation and operational effects.
