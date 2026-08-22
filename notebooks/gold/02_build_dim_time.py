# Databricks notebook source
# MAGIC %run ../utilities/bronze_common

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
source_path = "generated://minute_of_day"
target_path = gold_path(GOLD_TIME_RELATIVE_PATH, ADLS_ROOT)
audit_path = gold_path(GOLD_PIPELINE_AUDIT_RELATIVE_PATH, ADLS_ROOT)
started_at = utc_now()
started_clock = time.monotonic()
row_count = None
version = None

try:
    dimension = build_dim_time(spark, run_id)
    row_count = dimension.count()
    if row_count != 1_440 or dimension.select("time_key").distinct().count() != row_count:
        raise RuntimeError("Gold time dimension must contain 1,440 unique minute keys")
    version = schema_version(dimension.schema.json())
    write_gold_snapshot(dimension, target_path)
    if spark.read.format("delta").load(target_path).count() != row_count:
        raise RuntimeError("Persisted Gold time dimension count mismatch")
    completed_at = utc_now()
    append_gold_audit(
        spark,
        audit_path,
        GoldAuditRecord(
            run_id, "gold_build_dim_time", "dim_time", source_path, target_path,
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
            run_id, "gold_build_dim_time", "dim_time", source_path, target_path,
            started_at, completed_at, "FAILED", row_count, "FAILED", version,
            int((time.monotonic() - started_clock) * 1_000), sanitized_error(exc),
        ),
    )
    raise
