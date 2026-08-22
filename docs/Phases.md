# UrbanFlow Implementation Roadmap

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

- **Objective:** Provision a secure Azure landing zone for the data lake.
- **Major tasks:** Define naming and environments; create or approve resource group, storage account, hierarchical namespace, containers, identities, access assignments, lifecycle rules, and budget controls.
- **Expected deliverables:** Reachable ADLS Gen2 paths, access model, and documented configuration.
- **Dependencies:** Phases 1-2 and Azure access.
- **Manual cloud work:** Yes—subscription selection, permissions, security review, and resource creation require explicit approval.

## 4. Bronze ingestion

- **Objective:** Land immutable real source data and create traceable Bronze Delta tables.
- **Major tasks:** Implement parameterized ingestion, source validation, metadata capture, partition layout, idempotent writes, and run auditing.
- **Expected deliverables:** Real TLC and weather data in landing/Bronze storage with repeatable ingestion tests.
- **Dependencies:** Phases 2-3.
- **Manual cloud work:** Yes—credentials/managed identity and execution configuration must be established.

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
