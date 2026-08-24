# Staging models

The seven Phase 8 staging models are thin, deterministic views over the explicit
`urbanflow_analytics` dbt source. They preserve every Phase 7 column and grain while exposing
lower-case column names. Business transformations and aggregate construction remain upstream.

Source and staging tests retain the governed fact/dimension key contracts, the six fact
relationships, all three aggregate composite keys, and the aggregate-to-date/location
relationships used by downstream joins. Phase 7 remains responsible for load reconciliation,
partition boundaries, audits, and idempotency.
