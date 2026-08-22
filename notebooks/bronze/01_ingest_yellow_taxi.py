# Databricks notebook source
# MAGIC %run ../utilities/bronze_common

# COMMAND ----------
# MAGIC %run ../utilities/audit

# COMMAND ----------
import time
import uuid

dbutils.widgets.text("source_year", "2026")
dbutils.widgets.text("source_month", "5")

source_year = int(dbutils.widgets.get("source_year"))
source_month = int(dbutils.widgets.get("source_month"))
validate_batch(source_year, source_month)

run_id = str(uuid.uuid4())
pipeline_name = "bronze_ingest_yellow_taxi"
dataset = "yellow_taxi"
source_path = yellow_raw_path(source_year, source_month)
target_path = adls_path(YELLOW_DELTA_RELATIVE_PATH)
audit_path = adls_path(PIPELINE_AUDIT_RELATIVE_PATH)
started_at = utc_now()
started_clock = time.monotonic()
row_count = None
version = None

try:
    source_df = spark.read.parquet(source_path)
    missing = missing_columns(source_df.columns, YELLOW_REQUIRED_COLUMNS)
    if missing:
        raise ValueError(f"Yellow Taxi source is missing required columns: {missing}")

    row_count = source_df.count()
    if row_count == 0:
        raise ValueError("Yellow Taxi source is readable but contains zero rows")

    version = schema_version(source_df.schema.json())
    bronze_df = add_ingestion_metadata(
        source_df,
        run_id=run_id,
        year=source_year,
        month=source_month,
    )
    write_yellow_delta(bronze_df, target_path, source_year, source_month)

    written_count = spark.read.format("delta").load(target_path).where(
        yellow_replace_where(source_year, source_month)
    ).count()
    if written_count != row_count:
        raise RuntimeError(f"Batch verification failed: source={row_count}, target={written_count}")

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
            quality_status="NOT_EVALUATED",
            error=None,
            duration_ms=int((time.monotonic() - started_clock) * 1_000),
        ),
    )
    print({"run_id": run_id, "status": "SUCCEEDED", "row_count": row_count})
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
            quality_status="NOT_EVALUATED",
            error=sanitized_error(exc),
            duration_ms=int((time.monotonic() - started_clock) * 1_000),
        ),
    )
    raise
