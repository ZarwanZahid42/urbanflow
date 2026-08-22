# UrbanFlow Implementation Roadmap

**Current phase status (2026-08-23):** Phase 4 Databricks Bronze processing is complete and live-validated on Unity Catalog Serverless compute. Yellow Taxi raw and Delta counts reconcile at 4,090,836; taxi-zone raw and Delta counts reconcile at 265; two successful Yellow Taxi ingestion audits confirm retry idempotency. The expected quality status is `WARNING` because real source records include negative fare and total amounts. Local validation passes 35 tests. ADF, Silver, Gold, Snowflake, dbt, and Power BI remain unimplemented.

The roadmap is ordered to produce an interview-ready vertical slice quickly while keeping each external change explicit and controlled.

## 1. Project foundation

- **Objective:** Establish repository structure, scope, engineering rules, and technical direction.
- **Major tasks:** Create folders, project metadata, environment placeholders, architecture, requirements, design, and roadmap documents.
- **Expected deliverables:** Initialized local project and documentation foundation.
- **Dependencies:** None.
- **Manual cloud work:** No.

## 2. Real data acquisition

- **Objective:** Confirm stable, legal, reproducible access to TLC and weather data.
- **Major tasks:** Select representative TLC service types and periods; inspect schemas; select the real weather source; record URLs, cadence, licenses, and limits; build a small local acquisition proof without fabricating data.
- **Expected deliverables:** Source contracts, schema samples from real sources, acquisition configuration, and tested download logic.
- **Dependencies:** Phase 1.
- **Manual cloud work:** No; a weather-provider account or API key may require manual registration.

## 3. Azure/ADLS Gen2 setup

- **Status:** Complete for the Phase 3 scope; existing infrastructure and two uploaded Bronze files were verified live.
- **Objective:** Connect the local acquisition layer to the secure existing Azure data lake.
- **Major tasks:** Confirm the manually provisioned HNS-enabled account/filesystem, implement identity-authenticated ADLS clients, preserve Bronze partition paths, add staged chunk uploads, enforce existence/overwrite checks, audit outcomes, and validate May 2026 TLC/reference uploads.
- **Expected deliverables:** `DefaultAzureCredential` integration, tested file/directory uploader, ADLS Bronze paths, local upload audit, verified 69,699,174-byte TLC file, and verified 12,331-byte taxi-zone file.
- **Dependencies:** Phases 1-2, Azure CLI authentication, and existing data-plane permissions.
- **Manual cloud work:** Completed before application implementation: subscription/resource group selection, HNS storage account and filesystem creation, identity access configuration, and Azure CLI sign-in. Application code did not provision or reconfigure resources.

## 4. Bronze ingestion

- **Status:** Complete—implemented locally, synchronized to Databricks, and live-validated on Unity Catalog Serverless compute.
- **Objective:** Convert the two existing immutable raw TLC objects into traceable Bronze Delta datasets and publish report-only quality/audit results.
- **Implemented deliverables:** Parameterized Yellow Taxi ingestion; taxi-zone ingestion; required-schema and nonempty-source gates; technical metadata; year/month Yellow partition replacement; unpartitioned reference replacement; report-only quality metrics; structured Delta audit records; a live reconciliation notebook; and locally testable pure-Python contracts.
- **Dependencies:** Phases 2-3, `dbw-urbanflow`, Access Connector `ac-urbanflow`, and the existing raw objects.
- **Workspace configuration performed:** Unity Catalog storage credential `urbanflow_adls_managed_identity`, external location `urbanflow_adls_root`, workspace folder `/Users/zarwanzahid42@gmail.com/UrbanFlow/Phase4`, and a single-node validation cluster with 20-minute auto-termination. No Azure resource was created, modified, or deleted.
- **Exact manual equivalent:** Catalog **Connect > Credentials** → create an Azure managed-identity storage credential referencing the full `ac-urbanflow` resource ID; Catalog **External data > External locations** → create an external location for `abfss://urbanflow@urbanflowdata2026.dfs.core.windows.net/` using that credential. The Access Connector RBAC assignment is an Azure prerequisite and was already complete.
- **Validation status:** Final Serverless validation succeeded. Yellow Taxi reconciled 4,090,836 raw rows to 4,090,836 Delta rows; taxi zones reconciled 265 raw rows to 265 Delta rows. Yellow Taxi uses the two intended source-period partitions and taxi zones have none. Two successful Yellow Taxi ingestion audits were checked. Quality completed with expected `WARNING`: 14,231 negative fare amounts and 14,877 negative total amounts; duplicate, invalid-timestamp, negative-passenger, and all requested null counts were zero. The complete local suite passes 35 tests.
- **Scope boundary:** Weather Bronze, Silver, Gold, quarantine, ADF orchestration, Snowflake, dbt, and Power BI are not part of this phase.

## 5. Databricks Silver transformations

- **Objective:** Produce clean, typed, deduplicated, conformed datasets.
- **Major tasks:** Configure Databricks access; implement PySpark schema enforcement, normalization, validation, quarantine, deduplication, zone enrichment, and incremental Delta merges.
- **Expected deliverables:** Silver Delta tables, transformation jobs, tests, and quality metrics.
- **Dependencies:** Phase 4 and approved Databricks workspace/compute.
- **Manual cloud work:** Yes—workspace access, identity, secret integration, policies, and job compute need configuration.

## 6. Gold dimensional model

- **Objective:** Create business-ready mobility and weather models.
- **Major tasks:** Define grain and keys; build trip fact, date/time, zone, service, rate-code, payment, and weather dimensions; create useful aggregates; validate measures.
- **Expected deliverables:** Gold Delta fact/dimension tables, model documentation, and reconciliation tests.
- **Dependencies:** Phase 5 and agreed business definitions.
- **Manual cloud work:** Limited to running approved Databricks jobs and managing compute.

## 7. Snowflake integration

- **Objective:** Publish curated lake data to the analytical warehouse.
- **Major tasks:** Configure database, schemas, warehouse, roles, storage integration or approved transfer method; implement incremental loads and reconciliations.
- **Expected deliverables:** Secure Snowflake landing/curated tables, load process, and audit results.
- **Dependencies:** Phase 6 and Snowflake account access.
- **Manual cloud work:** Yes—account, role, warehouse, integration, and network/security configuration require explicit approval.

## 8. dbt transformations/tests

- **Objective:** Manage warehouse presentation logic, testing, lineage, and documentation as code.
- **Major tasks:** Initialize the dbt project; configure environments securely; create staging and mart models; add source, schema, relationship, freshness, and business-rule tests; generate docs.
- **Expected deliverables:** Versioned dbt project, passing tests, lineage graph, and model documentation.
- **Dependencies:** Phase 7.
- **Manual cloud work:** Yes—Snowflake role grants and secure CI credentials; dbt Cloud is optional and only added if explicitly selected.

## 9. Azure Data Factory orchestration

- **Objective:** Coordinate the end-to-end incremental workflow.
- **Major tasks:** Create parameterized linked services, datasets, pipelines, triggers, dependencies, retries, and notifications; invoke Databricks and warehouse/dbt steps through approved patterns.
- **Expected deliverables:** Deployable ADF definitions and a successful orchestrated run.
- **Dependencies:** Phases 4-8.
- **Manual cloud work:** Yes—ADF resources, connections, managed identities, triggers, and deployment permissions require explicit approval.

## 10. Data quality and monitoring

- **Objective:** Make correctness and operational health visible.
- **Major tasks:** Consolidate quality checks, define thresholds and SLAs, publish audit tables, configure alerts, create runbooks, and test failure/recovery paths.
- **Expected deliverables:** Quality scorecards, monitoring views, alerts, audit history, and recovery documentation.
- **Dependencies:** Phases 4-9.
- **Manual cloud work:** Yes—Azure monitoring/alert destinations and Snowflake monitoring privileges may require configuration.

## 11. CI/CD

- **Objective:** Automate validation and safe delivery of repository artifacts.
- **Major tasks:** Add linting, unit tests, dbt validation, artifact checks, environment promotion rules, container definitions where useful, and GitHub Actions workflows.
- **Expected deliverables:** Passing CI, controlled deployment workflows, documented secrets, and rollback approach.
- **Dependencies:** Stable implementation from Phases 4-10 and a GitHub repository.
- **Manual cloud work:** Yes—GitHub environments, federated identities or scoped secrets, approvals, and deployment permissions need manual setup.

## 12. Power BI analytics

- **Objective:** Deliver decision-ready NYC mobility reporting.
- **Major tasks:** Define the semantic model and measures; connect to Snowflake; build demand, revenue, geography, time, and weather views; validate totals and refresh behavior.
- **Expected deliverables:** Power BI model/dashboard, measure catalog, refresh configuration, and screenshots or demo assets.
- **Dependencies:** Phases 7-10.
- **Manual cloud work:** Yes—Power BI workspace, gateway if required, credentials, refresh schedule, and publishing permissions need setup.

## 13. Final documentation and portfolio polish

- **Objective:** Present an accurate, reproducible, interview-ready project narrative.
- **Major tasks:** Finalize diagrams, setup and operations guides, data dictionary, lineage, cost/security notes, demo script, screenshots, limitations, and future work; reconcile claims against implementation.
- **Expected deliverables:** Complete README and documentation, architecture visuals, reproducible demo, and portfolio summary.
- **Dependencies:** All implemented phases.
- **Manual cloud work:** Possibly—capture approved portal/dashboard evidence and verify public-sharing settings; no resource mutation unless explicitly requested.
