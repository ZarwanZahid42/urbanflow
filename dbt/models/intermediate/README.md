# Intermediate models

`int_trip_enriched` is the only justified intermediate model. At one row per trip, it joins
`stg_fact_trips` to the date, minute, and location dimensions in both pickup and drop-off roles.
This centralizes six conformed role-playing joins and their naming once for detailed BI models
without recalculating any Phase 6/7 measure.

The model is ephemeral: dbt retains its lineage and compiles it into consumers, while Snowflake
does not receive another persisted copy of the full fact. A row-count test plus key and resolved
dimension tests guard against join loss or multiplication.
