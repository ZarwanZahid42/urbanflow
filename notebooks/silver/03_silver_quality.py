# Databricks notebook source
# MAGIC %run ../utilities/bronze_common

# COMMAND ----------
# MAGIC %run ../utilities/silver_common

# COMMAND ----------
# MAGIC %run ../utilities/silver_audit

# COMMAND ----------
import time
import uuid

from pyspark.sql import functions as F

dbutils.widgets.text("source_year", "2026")
dbutils.widgets.text("source_month", "5")

source_year = int(dbutils.widgets.get("source_year"))
source_month = int(dbutils.widgets.get("source_month"))
validate_batch(source_year, source_month)

run_id = str(uuid.uuid4())
pipeline_name = "silver_quality"
dataset = "fact_trips"
bronze_path = adls_path(YELLOW_DELTA_RELATIVE_PATH)
fact_path = silver_path(SILVER_FACT_RELATIVE_PATH, ADLS_ROOT)
rejected_path = silver_path(SILVER_REJECTED_TRIPS_RELATIVE_PATH, ADLS_ROOT)
quality_path = silver_path(SILVER_QUALITY_AUDIT_RELATIVE_PATH, ADLS_ROOT)
audit_path = silver_path(SILVER_PIPELINE_AUDIT_RELATIVE_PATH, ADLS_ROOT)
started_at = utc_now()
started_clock = time.monotonic()
source_count = None
valid_count = None
rejected_count = None
version = None
result = "FAILED"

try:
    batch_filter = silver_replace_where(source_year, source_month)
    bronze = spark.read.format("delta").load(bronze_path).where(
        yellow_replace_where(source_year, source_month)
    )
    valid = spark.read.format("delta").load(fact_path).where(batch_filter)
    rejected = spark.read.format("delta").load(rejected_path).where(batch_filter)
    source_count = bronze.count()
    valid_count = valid.count()
    rejected_count = rejected.count()

    common_columns = [
        "pickup_datetime",
        "dropoff_datetime",
        "pickup_location_id",
        "dropoff_location_id",
        "passenger_count",
        "trip_distance",
        "fare_amount",
        "total_amount",
    ]
    all_classified = valid.select(*common_columns).unionByName(
        rejected.select(*common_columns)
    )
    observed = all_classified.agg(
        F.sum(F.when(F.col("pickup_datetime").isNull(), 1).otherwise(0)).alias(
            "null_pickup_timestamp_count"
        ),
        F.sum(F.when(F.col("dropoff_datetime").isNull(), 1).otherwise(0)).alias(
            "null_dropoff_timestamp_count"
        ),
        F.sum(F.when(F.col("pickup_location_id").isNull(), 1).otherwise(0)).alias(
            "null_pickup_location_count"
        ),
        F.sum(F.when(F.col("dropoff_location_id").isNull(), 1).otherwise(0)).alias(
            "null_dropoff_location_count"
        ),
        F.sum(
            F.when(F.col("dropoff_datetime") < F.col("pickup_datetime"), 1).otherwise(0)
        ).alias("invalid_timestamp_count"),
        F.sum(F.when(F.col("passenger_count").isNull(), 1).otherwise(0)).alias(
            "null_passenger_count"
        ),
        F.sum(F.when(F.col("passenger_count") < 0, 1).otherwise(0)).alias(
            "negative_passenger_count"
        ),
        F.sum(F.when(F.col("trip_distance") < 0, 1).otherwise(0)).alias(
            "negative_distance_count"
        ),
        F.sum(F.when(F.col("fare_amount") < 0, 1).otherwise(0)).alias(
            "negative_fare_amount_count"
        ),
        F.sum(F.when(F.col("total_amount") < 0, 1).otherwise(0)).alias(
            "negative_total_amount_count"
        ),
    ).first().asDict()

    duplicate_count = rejected.where(F.array_contains("rejection_rules", "DUPLICATE_TRIP")).count()
    invalid_pickup = rejected.where(
        F.array_contains("rejection_rules", "INVALID_PICKUP_LOCATION")
    ).count()
    invalid_dropoff = rejected.where(
        F.array_contains("rejection_rules", "INVALID_DROPOFF_LOCATION")
    ).count()
    metrics = {
        "source_row_count": source_count,
        "valid_row_count": valid_count,
        "rejected_row_count": rejected_count,
        "rejection_rate": rejected_count / source_count if source_count else 1.0,
        "duplicate_count": duplicate_count,
        "invalid_location_count": invalid_pickup + invalid_dropoff,
        "referential_integrity_failure_count": invalid_pickup + invalid_dropoff,
        **{name: int(value or 0) for name, value in observed.items()},
    }
    result = silver_quality_status(metrics)
    measured_at = utc_now()
    rows = []
    for name, value in sorted(metrics.items()):
        threshold = 0.20 if name == "rejection_rate" else 0.0
        if result == "FAILED" and name in {
            "source_row_count",
            "valid_row_count",
            "rejected_row_count",
            "rejection_rate",
        }:
            outcome = "FAILED"
        elif name not in {"source_row_count", "valid_row_count"} and float(value) > threshold:
            outcome = "WARNING"
        else:
            outcome = "PASS"
        rows.append(
            {
                "run_id": run_id,
                "dataset": dataset,
                "measured_at_utc": measured_at,
                "metric_name": name,
                "metric_value": float(value),
                "threshold": threshold,
                "outcome": outcome,
            }
        )
    append_silver_quality_rows(spark, quality_path, rows)

    version = schema_version(valid.schema.json())
    if result == "FAILED":
        raise RuntimeError("Silver quality thresholds failed")
    completed_at = utc_now()
    append_silver_audit(
        spark,
        audit_path,
        SilverAuditRecord(
            run_id=run_id,
            pipeline_name=pipeline_name,
            dataset=dataset,
            source_path=fact_path,
            target_path=quality_path,
            started_at_utc=started_at,
            completed_at_utc=completed_at,
            source_row_count=source_count,
            valid_row_count=valid_count,
            rejected_row_count=rejected_count,
            quality_status=result,
            schema_version=version,
            duration_ms=int((time.monotonic() - started_clock) * 1_000),
            error=None,
        ),
    )
    print({"run_id": run_id, "quality_status": result, **metrics})
except Exception as exc:
    if result != "FAILED":
        result = "FAILED"
    completed_at = utc_now()
    append_silver_audit(
        spark,
        audit_path,
        SilverAuditRecord(
            run_id=run_id,
            pipeline_name=pipeline_name,
            dataset=dataset,
            source_path=fact_path,
            target_path=quality_path,
            started_at_utc=started_at,
            completed_at_utc=completed_at,
            source_row_count=source_count,
            valid_row_count=valid_count,
            rejected_row_count=rejected_count,
            quality_status="FAILED",
            schema_version=version,
            duration_ms=int((time.monotonic() - started_clock) * 1_000),
            error=sanitized_error(exc),
        ),
    )
    raise
