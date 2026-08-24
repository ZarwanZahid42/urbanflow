# UrbanFlow dbt

This directory contains the Phase 8 dbt Core project scaffold. Initialization does not
include production models, tests, seeds, snapshots, packages, or a live Snowflake connection.

The dbt project and profile name are both `urbanflow`. `URBANFLOW.ANALYTICS` remains the
governed Phase 7 upstream contract; later Phase 8 work will declare its seven relations as
sources and build downstream staging and presentation models without mutating Phase 7 tables.

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
