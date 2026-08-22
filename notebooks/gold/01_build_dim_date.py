# Databricks notebook source
# MAGIC %run ../utilities/bronze_common

# COMMAND ----------
# MAGIC %run ../utilities/silver_common

# COMMAND ----------
# MAGIC %run ../utilities/gold_common

# COMMAND ----------
# MAGIC %run ../utilities/gold_transformations

# COMMAND ----------
# MAGIC %run ../utilities/gold_audit

# COMMAND ----------
import time
import uuid

run_id = str(uuid.uuid4())
source_path = silver_path(SILVER_FACT_RELATIVE_PATH, ADLS_ROOT)
target_path = gold_path(GOLD_DATE_RELATIVE_PATH, ADLS_ROOT)
audit_path = gold_path(GOLD_PIPELINE_AUDIT_RELATIVE_PATH, ADLS_ROOT)
started_at = utc_now()
started_clock = time.monotonic()
row_count = None
version = None

try:
    silver_trips = spark.read.format("delta").load(source_path)
    dimension = build_dim_date(silver_trips, run_id)
    row_count = dimension.count()
    if row_count == 0:
        raise ValueError("Gold date dimension contains zero rows")
    if dimension.select("date_key").distinct().count() != row_count:
        raise RuntimeError("Gold date keys are not unique")
    version = schema_version(dimension.schema.json())
    write_gold_snapshot(dimension, target_path)
    if spark.read.format("delta").load(target_path).count() != row_count:
        raise RuntimeError("Persisted Gold date dimension count mismatch")
    completed_at = utc_now()
    append_gold_audit(
        spark,
        audit_path,
        GoldAuditRecord(
            run_id, "gold_build_dim_date", "dim_date", source_path, target_path,
            started_at, completed_at, "SUCCEEDED", row_count, "PASS", version,
            int((time.monotonic() - started_clock) * 1_000), None,
        ),
    )
    print({"run_id": run_id, "status": "SUCCEEDED", "row_count": row_count})
except Exception as exc:
    completed_at = utc_now()
    append_gold_audit(
        spark,
        audit_path,
        GoldAuditRecord(
            run_id, "gold_build_dim_date", "dim_date", source_path, target_path,
            started_at, completed_at, "FAILED", row_count, "FAILED", version,
            int((time.monotonic() - started_clock) * 1_000), sanitized_error(exc),
        ),
    )
    raise
