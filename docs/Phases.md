# UrbanFlow Implementation Roadmap

**Current phase status (2026-08-24):** Phases 1-8 are complete and live-validated. Phase 9 concluded as a productionization review with Azure Data Factory intentionally deferred; Phase 10 has not started. Phase 7 commit `ea0f67f` contains the governed Snowflake source contract. Phase 8 completed `dbt debug`, parse, build, standalone tests, live relation validation, Phase 7 reconciliation, and documentation generation against the separate `URBANFLOW.DBT_DEV` target. The build created 11 views, kept the intermediate model ephemeral, and passed 95/95 tests. Power BI and all later productionization items remain unimplemented.

The roadmap preserves completed implementation history and labels all post-Phase-9 items as future work. Every external change remains explicit and controlled.

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

- **Status:** Complete, live-validated, and committed in `e2b7c49`.
- **Objective:** Produce typed, standardized, deduplicated, referentially valid trip facts and taxi-zone dimensions without losing rejected source records.
- **Implemented deliverables:** Four Serverless notebooks; reusable Silver rules, transformations, and audit utilities; Delta fact/dimension/quarantine/audit targets; structured multi-rule rejection; explicit Spark types; reference-driven zone validation; deterministic SHA-256 trip IDs; monthly partition replacement; snapshot replacement for zones; and 13 new PySpark-free tests.
- **Validation:** Databricks job `713366891015169`, successful run `841707541463751`, processed the full notebook order twice where required. Counts were 4,090,836 Bronze trips = 4,090,836 valid + 0 rejected, and 265 Bronze zones = 265 valid + 0 rejected. Two stable fact audits, zero duplicate valid trip IDs, zero referential failures, correct partitions, schemas, and audit tables were verified.
- **Quality:** `WARNING` with zero rejection rate. Preserved observations were 955,371 null passenger counts, 14,231 negative fares, and 14,877 negative totals. All requested null timestamp/location, invalid timestamp/location, negative passenger/distance, duplicate, and referential-failure counts were zero.
- **Dependencies:** Completed Phase 4 Bronze Delta, Unity Catalog external location `urbanflow_adls_root`, and Databricks Serverless.
- **Cloud work:** Eight notebooks and one stopped Serverless validation job were created in the Databricks workspace. No Azure infrastructure, keys, SAS tokens, connection strings, passwords, client secrets, or service principals were created or changed.
- **Scope boundary:** Gold, Snowflake, dbt, ADF, Power BI, and unrelated future work remain unimplemented.

## 6. Gold dimensional model

- **Status:** Complete, live-validated, and committed in `999deb6`.
- **Objective:** Publish a business-ready TLC mobility fact, reusable date/time/location dimensions, and reconciled daily/location/hourly aggregates.
- **Implemented deliverables:** Seven ordered notebooks; reusable Gold rules, Spark transformations, and audit contracts; seven Delta analytical tables; Gold pipeline and quality audit paths; guarded derived measures; explicit financial-adjustment measures; batch replacement; and 14 new PySpark-free tests.
- **Validation:** Serverless job `631815480020120`, successful two-pass runs `191548502476871` and `150700319211970`, reconciled 4,090,836 Silver trips to 4,090,836 unique Gold facts. Dimensions contain 6,363 dates, 1,440 minutes, and 265 locations. Aggregates contain 35 daily, 265 location, and 748 hourly rows; daily, pickup, dropoff, and hourly trip counts each reconcile to 4,090,836.
- **Quality:** `WARNING` only for 955,371 null passenger counts and 14,953 financial-adjustment trips. Duplicate/null critical keys, dimension duplicates, date/time/location referential failures, impossible duration/distance, invalid derived metrics, schema failures, empty facts, and all three aggregation-reconciliation failures are zero.
- **Idempotency:** Facts and aggregates replace only `source_year=2026/source_month=5`; dimensions are deterministic snapshots. Final validation checked at least two matching successful fact audits and stable cardinality.
- **Cloud work:** Twelve sources were synchronized under `/Users/zarwanzahid42@gmail.com/UrbanFlow/Phase6` and one stopped Serverless validation job was created. No Azure infrastructure or secret-based authentication changed.
- **Scope boundary:** Weather Gold, Snowflake, dbt, ADF, Power BI, and unrelated future work remain unimplemented.

## 7. Snowflake integration

- **Status:** Complete—implemented locally, synchronized to Databricks, and live-validated through two full Snowflake passes.
- **Objective:** Publish the seven live-validated Gold Delta datasets into governed Snowflake landing, analytical, and audit schemas.
- **Implemented deliverables:** Explicit ANALYTICS/LANDING/AUDIT DDL; key-pair configuration contract; fail-closed RSA PKCS#8 normalization for the Spark connector; isolated Serverless connector options; ten ordered notebooks; validated landing gates; transactional period replacement for facts/aggregates; deterministic dimension snapshots; count, uniqueness, boundary, referential, and aggregate reconciliation; failure audits; and two-pass idempotency validation.
- **Loading boundary:** Data transfer uses Snowflake's internally managed staging through the Spark connector. No Azure storage integration, external ADLS stage, SAS token, storage key, password, service principal, client secret, or new Azure resource is introduced.
- **Dependencies:** Completed Phase 6; existing `URBANFLOW` objects, loader/reader roles, warehouse, and RSA-configured service user; a Databricks Serverless environment with `snowflake-connector-python`; and the documented secret scope.
- **Manual cloud work:** Completed for the current scope: Snowflake objects/grants and Databricks-backed scope `urbanflow-snowflake` are configured. The non-sensitive `snowflake_schema` setting was corrected to `ANALYTICS`; the stored private key and all credentials were left unchanged.
- **Validation:** Databricks job `957309293840081`, run `306537529517430`, completed all 17 tasks. Landing and target counts match for all seven datasets: 4,090,836 facts, 6,363 dates, 1,440 minutes, 265 locations, 35 daily aggregates, 265 location aggregates, and 748 hourly aggregates. Duplicate keys, fact foreign-key failures, source-boundary failures, aggregate-total failures, audit failures, and all 40 reconciliation failures are zero. Both passes produced one stable target count for every dataset; `idempotency_final` passed. The completion gate was satisfied live on 2026-08-24.
- **Commit:** `ea0f67f feat: complete phase 7 snowflake validation`.

## 8. dbt transformations/tests

- **Status:** Complete and live-validated on Snowflake. Seven ANALYTICS sources, seven staging views, one ephemeral intermediate model, four mart views, generic/singular tests, static architecture tests, and repository documentation are implemented.
- **Objective:** Manage warehouse presentation logic, testing, lineage, and documentation as code.
- **Implemented models:** `int_trip_enriched` centralizes the six pickup/drop-off role-playing dimension joins at trip grain. `mart_trip_details`, `mart_daily_mobility`, `mart_hourly_mobility`, and `mart_location_mobility` expose declared BI grains while preserving Phase 7 measures and adjustment semantics.
- **Materializations:** Staging and marts are deterministic views. The trip-enrichment intermediate is ephemeral to avoid another persisted full-fact copy. No incremental state is introduced without live workload evidence and a bounded idempotency contract.
- **Tests:** Existing source/staging keys and fact relationships remain; aggregate-to-dimension relationships, intermediate row preservation, mart keys, governed time-of-day values, and focused count rules are added. Phase 7 audit, reconciliation, partition, and idempotency tests remain upstream and are not duplicated.
- **Ownership boundary:** Phase 7 LANDING, ANALYTICS, and AUDIT tables remain governed upstream contracts. dbt reads ANALYTICS and publishes downstream relations in a separately approved target schema; it does not replace Databricks Silver/Gold or mutate Phase 7 tables to make tests pass.
- **Validation:** The non-sensitive configuration gate passed with eight external `DBT_*` variables, an external profile and private key, and `DBT_TARGET_SCHEMA=DBT_DEV`. `dbt debug` and parse passed. `dbt build` created 11 views and completed 106/106 resources: 11 model successes and 95 test passes, with zero warnings, errors, or skips. A separate `dbt test` also passed 95/95.
- **Live reconciliation:** `DBT_DEV` contains exactly the seven staging and four mart views; `int_trip_enriched` remains ephemeral. All seven staging row counts and all four mart row counts match Phase 7, including 4,090,836 trip details and 35/748/265 daily/hourly/location rows. Exact daily, hourly, and location measure mismatches are zero. Generated catalog, manifest, and documentation index were written outside the repository.
- **Dependencies:** Completed Phase 7 commit `ea0f67f`; Python 3.11+; the constrained dbt 1.9/Snowflake adapter line; Snowflake network/account access; and manually approved least-privilege access to the existing database, warehouse, ANALYTICS sources, and selected target schema.
- **Manual cloud work:** Completed for this validation by the user: `URBANFLOW.DBT_DEV`, key-pair authentication, and the required source-read/target-create boundary were prepared externally. Validation used `SECURITYADMIN` and `COMPUTE_WH` exactly as configured; Codex did not change roles or privileges. No write privilege or mutation was applied to ANALYTICS. dbt Cloud remains optional and requires explicit selection.
## 9. Productionization review and scope finalization

- **Status:** Deferred / scope finalized.
- **Objective:** Review whether another cloud orchestration layer materially improves the completed portfolio and record the productionization boundary accurately.
- **Decision:** Azure Data Factory was evaluated as a possible coordinator for acquisition, Databricks, Snowflake, and dbt, but was intentionally deferred.
- **Rationale:** The validated Python → ADLS Gen2 → Databricks → Snowflake → dbt path already demonstrates ingestion, distributed processing, data quality, idempotency, reconciliation, transactional loading, and analytical modeling. ADF would currently add infrastructure, permissions, deployment configuration, and recurring cloud-operation scope without materially expanding those demonstrated capabilities.
- **Evidence boundary:** No ADF factory, linked service, dataset, pipeline, trigger, identity assignment, deployment, or orchestrated run is implemented or claimed.
- **Future rule:** Resume ADF only on explicit user instruction. Do not create or infer cloud resource identifiers.

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
