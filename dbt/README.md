# UrbanFlow dbt

This directory contains the Phase 8 dbt Core project. The implemented local layer declares
the seven governed Phase 7 ANALYTICS sources and exposes conservative staging views with
contract-backed tests. It does not include intermediate models, marts, seeds, snapshots,
packages, or a live Snowflake connection.

The dbt project and profile name are both `urbanflow`. Source group
`urbanflow_analytics` maps logical aggregate names such as `agg_daily` to the physical Phase 7
relations such as `AGG_DAILY_TRIPS`. Staging models select and rename every contracted column
without filtering, aggregating, or mutating the upstream tables.

## Safe local configuration

1. Install the repository requirements into the project `.venv`.
2. Set the `DBT_SNOWFLAKE_*` and `DBT_TARGET_SCHEMA` variables documented in
   `.env.example` through the local shell or an ignored `.env` file.
3. Copy `profiles.yml.example` to an ignored `dbt/profiles.yml`, or place an equivalent
   `profiles.yml` in the external dbt profiles directory.
4. Run local commands from this directory. Do not run `dbt run`, `dbt build`, `dbt test`,
   `dbt seed`, or `dbt snapshot` until Snowflake access and the target schema are explicitly
   approved.

No account identifier, user, role, warehouse, target schema, private-key path, or credential
is populated by this scaffold.
