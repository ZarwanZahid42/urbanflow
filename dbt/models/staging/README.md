# Staging models

Phase 8 staging models are thin, deterministic views over the explicit
`urbanflow_analytics` dbt source. They preserve every Phase 7 column and grain while exposing
lower-case column names. Business transformations and aggregate construction remain upstream.
