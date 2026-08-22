# Databricks notebook source
# MAGIC %run ../utilities/bronze_common

# COMMAND ----------
# MAGIC %run ../utilities/audit

# COMMAND ----------
# MAGIC %run ../utilities/quality

# COMMAND ----------
import time
import uuid

dbutils.widgets.text("source_year", "2026")
dbutils.widgets.text("source_month", "5")

source_year = int(dbutils.widgets.get("source_year"))
source_month = int(dbutils.widgets.get("source_month"))
validate_batch(source_year, source_month)

run_id = str(uuid.uuid4())
pipeline_name = "bronze_quality"
dataset = "yellow_taxi"
source_path = adls_path(YELLOW_DELTA_RELATIVE_PATH)
target_path = adls_path(QUALITY_AUDIT_RELATIVE_PATH)
audit_path = adls_path(PIPELINE_AUDIT_RELATIVE_PATH)
started_at = utc_now()
started_clock = time.monotonic()
row_count = None
version = None
result = None

try:
    bronze_df = spark.read.format("delta").load(source_path).where(
        yellow_replace_where(source_year, source_month)
    )
    missing = missing_columns(bronze_df.columns, YELLOW_REQUIRED_COLUMNS)
    if missing:
        raise ValueError(f"Yellow Taxi Bronze Delta is missing required columns: {missing}")

    version = schema_version(bronze_df.schema.json())
    original_columns = [column for column in bronze_df.columns if column not in TECHNICAL_COLUMNS]
    metrics = evaluate_yellow_quality(bronze_df, original_columns)
    row_count = metrics["row_count"]
    if row_count == 0:
        raise ValueError("Yellow Taxi Bronze batch contains zero rows")

    result = quality_status(metrics, YELLOW_WARNING_METRICS)
    spark.createDataFrame(quality_rows(run_id, dataset, metrics)).write.format("delta").mode(
        "append"
    ).save(target_path)

    completed_at = utc_now()
    append_audit_record(
        spark,
        audit_path,
        AuditRecord(
            run_id=run_id,
            pipeline_name=pipeline_name,
            dataset=dataset,
            source_path=source_path,
            target_path=target_path,
            started_at_utc=started_at,
            completed_at_utc=completed_at,
            status="SUCCEEDED",
            row_count=row_count,
            schema_version=version,
            quality_status=result,
            error=None,
            duration_ms=int((time.monotonic() - started_clock) * 1_000),
        ),
    )
    print({"run_id": run_id, "quality_status": result, **metrics})
except Exception as exc:
    completed_at = utc_now()
    append_audit_record(
        spark,
        audit_path,
        AuditRecord(
            run_id=run_id,
            pipeline_name=pipeline_name,
            dataset=dataset,
            source_path=source_path,
            target_path=target_path,
            started_at_utc=started_at,
            completed_at_utc=completed_at,
            status="FAILED",
            row_count=row_count,
            schema_version=version,
            quality_status=result or "NOT_EVALUATED",
            error=sanitized_error(exc),
            duration_ms=int((time.monotonic() - started_clock) * 1_000),
        ),
    )
    raise
