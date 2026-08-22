# Databricks notebook source
# MAGIC %run ../utilities/bronze_common

# COMMAND ----------
# MAGIC %run ../utilities/silver_common

# COMMAND ----------
# MAGIC %run ../utilities/silver_transformations

# COMMAND ----------
# MAGIC %run ../utilities/silver_audit

# COMMAND ----------
import time
import uuid

run_id = str(uuid.uuid4())
pipeline_name = "silver_transform_taxi_zones"
dataset = "dim_taxi_zones"
source_path = adls_path(ZONES_DELTA_RELATIVE_PATH)
target_path = silver_path(SILVER_ZONES_RELATIVE_PATH, ADLS_ROOT)
rejected_path = silver_path(SILVER_REJECTED_ZONES_RELATIVE_PATH, ADLS_ROOT)
audit_path = silver_path(SILVER_PIPELINE_AUDIT_RELATIVE_PATH, ADLS_ROOT)
started_at = utc_now()
started_clock = time.monotonic()
source_row_count = None
valid_row_count = None
rejected_row_count = None
version = None

try:
    bronze_zones = spark.read.format("delta").load(source_path)
    valid_zones, rejected_zones = build_zone_frames(bronze_zones, run_id)
    source_row_count = bronze_zones.count()
    valid_row_count = valid_zones.count()
    rejected_row_count = rejected_zones.count()
    if source_row_count == 0:
        raise ValueError("Taxi-zone Bronze Delta contains zero rows")
    if source_row_count != valid_row_count + rejected_row_count:
        raise RuntimeError("Taxi-zone valid/rejected counts do not reconcile to Bronze")

    version = schema_version(valid_zones.schema.json())
    write_silver_snapshot(valid_zones, target_path)
    write_silver_snapshot(rejected_zones, rejected_path)
    if spark.read.format("delta").load(target_path).count() != valid_row_count:
        raise RuntimeError("Persisted Silver taxi-zone count mismatch")
    if spark.read.format("delta").load(rejected_path).count() != rejected_row_count:
        raise RuntimeError("Persisted rejected taxi-zone count mismatch")

    quality = "WARNING" if rejected_row_count else "PASS"
    completed_at = utc_now()
    append_silver_audit(
        spark,
        audit_path,
        SilverAuditRecord(
            run_id=run_id,
            pipeline_name=pipeline_name,
            dataset=dataset,
            source_path=source_path,
            target_path=target_path,
            started_at_utc=started_at,
            completed_at_utc=completed_at,
            source_row_count=source_row_count,
            valid_row_count=valid_row_count,
            rejected_row_count=rejected_row_count,
            quality_status=quality,
            schema_version=version,
            duration_ms=int((time.monotonic() - started_clock) * 1_000),
            error=None,
        ),
    )
    print(
        {
            "run_id": run_id,
            "status": "SUCCEEDED",
            "source_row_count": source_row_count,
            "valid_row_count": valid_row_count,
            "rejected_row_count": rejected_row_count,
        }
    )
except Exception as exc:
    completed_at = utc_now()
    append_silver_audit(
        spark,
        audit_path,
        SilverAuditRecord(
            run_id=run_id,
            pipeline_name=pipeline_name,
            dataset=dataset,
            source_path=source_path,
            target_path=target_path,
            started_at_utc=started_at,
            completed_at_utc=completed_at,
            source_row_count=source_row_count,
            valid_row_count=valid_row_count,
            rejected_row_count=rejected_row_count,
            quality_status="FAILED",
            schema_version=version,
            duration_ms=int((time.monotonic() - started_clock) * 1_000),
            error=sanitized_error(exc),
        ),
    )
    raise
