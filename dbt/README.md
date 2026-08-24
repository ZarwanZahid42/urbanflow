# UrbanFlow dbt

This directory contains the complete, live-validated Phase 8 dbt Core implementation. Seven governed
Phase 7 `URBANFLOW.ANALYTICS` relations feed seven conservative staging views, one reusable
trip-enrichment intermediate model, and four BI-facing mart views. Source and model tests,
model documentation, and `ref()` lineage are implemented. Controlled Snowflake validation in
`URBANFLOW.DBT_DEV` created 11 views, passed all 95 tests, reconciled to Phase 7, and generated
the dbt catalog without mutating ANALYTICS.

The dbt project and profile name are both `urbanflow`. Source group
`urbanflow_analytics` maps logical aggregate names such as `agg_daily` to the physical Phase 7
relations such as `AGG_DAILY_TRIPS`. Staging models select and rename every contracted column
without filtering, aggregating, or mutating the upstream tables.

`int_trip_enriched` is ephemeral because it centralizes six role-playing joins for downstream
SQL without creating another persisted copy of the 4.09-million-row fact. `mart_trip_details`,
`mart_daily_mobility`, `mart_hourly_mobility`, and `mart_location_mobility` are deterministic
views. The aggregate marts expose the authoritative Phase 7 measures with conformed descriptive
attributes; they do not recalculate Gold logic.

## Safe local configuration

1. Install the repository requirements into the project `.venv`.
2. Select an approved target schema and execution role, then obtain the required
   database, warehouse, source-read, and target-schema permissions manually.
3. Set the `DBT_SNOWFLAKE_*` and `DBT_TARGET_SCHEMA` variables documented in `.env.example`
   through the local shell or an ignored `.env` file.
4. Place an equivalent `profiles.yml` in the external dbt profiles directory; do not create a
   repository-local profile for live validation.
5. After access is approved, run `dbt debug`, `dbt parse`, `dbt build`, `dbt test`, and
   `dbt docs generate`, then reconcile the live results with the Phase 7 evidence. The completed
   validation used `DBT_DEV`; future runs must preserve the same ANALYTICS read-only boundary.

Do not run a live dbt command until Snowflake access and the target schema are explicitly
approved. No account identifier, user, role, warehouse, target schema, private-key path, or
credential is populated by the repository. Generated `target/`, `logs/`, `dbt_packages/`, and
rendered documentation artifacts remain untracked.
