# Mart models

The mart layer contains four deterministic Snowflake views with declared BI grains:

- `mart_trip_details`: one row per validated trip, with conformed pickup/drop-off attributes,
  authoritative trip measures, and source-to-Gold lineage.
- `mart_daily_mobility`: one row per source year, source month, and pickup date.
- `mart_hourly_mobility`: one row per source year, source month, pickup date, and hour.
- `mart_location_mobility`: one row per source year, source month, and TLC location.

The three aggregate marts add descriptive dimension attributes to the existing Phase 7 Gold
aggregates. They deliberately reuse those measures instead of recomputing daily, hourly, or
location business logic in dbt. Views provide deterministic, stateless builds for the current
scope; incremental state is not justified before live workload measurements and an approved
refresh design exist.
