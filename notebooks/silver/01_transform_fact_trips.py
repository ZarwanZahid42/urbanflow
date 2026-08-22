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

dbutils.widgets.text("source_year", "2026")
dbutils.widgets.text("source_month", "5")

source_year = int(dbutils.widgets.get("source_year"))
source_month = int(dbutils.widgets.get("source_month"))
validate_batch(source_year, source_month)

run_id = str(uuid.uuid4())
pipeline_name = "silver_transform_fact_trips"
dataset = "fact_trips"
source_path = adls_path(YELLOW_DELTA_RELATIVE_PATH)
zone_source_path = adls_path(ZONES_DELTA_RELATIVE_PATH)
target_path = silver_path(SILVER_FACT_RELATIVE_PATH, ADLS_ROOT)
rejected_path = silver_path(SILVER_REJECTED_TRIPS_RELATIVE_PATH, ADLS_ROOT)
audit_path = silver_path(SILVER_PIPELINE_AUDIT_RELATIVE_PATH, ADLS_ROOT)
started_at = utc_now()
started_clock = time.monotonic()
source_row_count = None
valid_row_count = None
rejected_row_count = None
version = None

try:
    bronze_trips = spark.read.format("delta").load(source_path).where(
        yellow_replace_where(source_year, source_month)
    )
    bronze_zones = spark.read.format("delta").load(zone_source_path)

    # The requested execution order runs trips before the persisted Silver dimension.
    # Apply the exact shared Silver zone contract in memory; notebook 02 persists it.
    valid_zones, _ = build_zone_frames(bronze_zones, run_id)
    valid_trips, rejected_trips = build_trip_frames(bronze_trips, valid_zones, run_id)

    source_row_count = bronze_trips.count()
    valid_row_count = valid_trips.count()
    rejected_row_count = rejected_trips.count()
    if source_row_count == 0:
        raise ValueError("Yellow Taxi Bronze batch contains zero rows")
    if source_row_count != valid_row_count + rejected_row_count:
        raise RuntimeError(
            "Trip reconciliation failed: "
            f"source={source_row_count}, valid={valid_row_count}, rejected={rejected_row_count}"
        )

    version = schema_version(valid_trips.schema.json())
    write_silver_partition(valid_trips, target_path, source_year, source_month)
    write_silver_partition(rejected_trips, rejected_path, source_year, source_month)

    persisted_valid = spark.read.format("delta").load(target_path).where(
        silver_replace_where(source_year, source_month)
    ).count()
    persisted_rejected = spark.read.format("delta").load(rejected_path).where(
        silver_replace_where(source_year, source_month)
    ).count()
    if persisted_valid != valid_row_count or persisted_rejected != rejected_row_count:
        raise RuntimeError("Persisted Silver trip counts do not match classified counts")

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
