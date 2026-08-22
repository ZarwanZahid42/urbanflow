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

dbutils.widgets.text("source_year", "2026")
dbutils.widgets.text("source_month", "5")
source_year = int(dbutils.widgets.get("source_year"))
source_month = int(dbutils.widgets.get("source_month"))
validate_batch(source_year, source_month)

run_id = str(uuid.uuid4())
source_path = silver_path(SILVER_FACT_RELATIVE_PATH, ADLS_ROOT)
target_path = gold_path(GOLD_FACT_RELATIVE_PATH, ADLS_ROOT)
audit_path = gold_path(GOLD_PIPELINE_AUDIT_RELATIVE_PATH, ADLS_ROOT)
started_at = utc_now()
started_clock = time.monotonic()
row_count = None
version = None

try:
    silver_fact = spark.read.format("delta").load(source_path).where(
        silver_replace_where(source_year, source_month)
    )
    source_count = silver_fact.count()
    fact = build_gold_fact(silver_fact, run_id)
    row_count = fact.count()
    if source_count == 0 or row_count != source_count:
        raise RuntimeError(f"Gold fact reconciliation failed: source={source_count}, gold={row_count}")
    if fact.select("trip_id").distinct().count() != row_count:
        raise RuntimeError("Gold fact trip IDs are not unique")
    missing = missing_required_columns(fact.columns, GOLD_FACT_REQUIRED_COLUMNS)
    if missing:
        raise RuntimeError(f"Gold fact schema is missing required columns: {missing}")
    version = schema_version(fact.schema.json())
    write_gold_partition(fact, target_path, source_year, source_month)
    persisted = spark.read.format("delta").load(target_path).where(
        gold_replace_where(source_year, source_month)
    ).count()
    if persisted != row_count:
        raise RuntimeError("Persisted Gold fact count mismatch")
    completed_at = utc_now()
    append_gold_audit(
        spark,
        audit_path,
        GoldAuditRecord(
            run_id, "gold_build_fact_trips", "fact_trips", source_path, target_path,
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
            run_id, "gold_build_fact_trips", "fact_trips", source_path, target_path,
            started_at, completed_at, "FAILED", row_count, "FAILED", version,
            int((time.monotonic() - started_clock) * 1_000), sanitized_error(exc),
        ),
    )
    raise
