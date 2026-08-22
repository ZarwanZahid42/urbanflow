# Databricks notebook source
# MAGIC %run ../utilities/bronze_common

# COMMAND ----------
# MAGIC %run ../utilities/gold_common

# COMMAND ----------
# MAGIC %run ../utilities/gold_audit

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
fact_path = gold_path(GOLD_FACT_RELATIVE_PATH, ADLS_ROOT)
date_path = gold_path(GOLD_DATE_RELATIVE_PATH, ADLS_ROOT)
time_path = gold_path(GOLD_TIME_RELATIVE_PATH, ADLS_ROOT)
location_path = gold_path(GOLD_LOCATION_RELATIVE_PATH, ADLS_ROOT)
daily_path = gold_path(GOLD_DAILY_RELATIVE_PATH, ADLS_ROOT)
location_agg_path = gold_path(GOLD_LOCATION_AGG_RELATIVE_PATH, ADLS_ROOT)
hourly_path = gold_path(GOLD_HOURLY_RELATIVE_PATH, ADLS_ROOT)
quality_path = gold_path(GOLD_QUALITY_AUDIT_RELATIVE_PATH, ADLS_ROOT)
audit_path = gold_path(GOLD_PIPELINE_AUDIT_RELATIVE_PATH, ADLS_ROOT)
batch_filter = gold_replace_where(source_year, source_month)
started_at = utc_now()
started_clock = time.monotonic()
row_count = None
version = None
result = "FAILED"

try:
    fact = spark.read.format("delta").load(fact_path).where(batch_filter)
    dates = spark.read.format("delta").load(date_path)
    times = spark.read.format("delta").load(time_path)
    locations = spark.read.format("delta").load(location_path)
    daily = spark.read.format("delta").load(daily_path).where(batch_filter)
    location_agg = spark.read.format("delta").load(location_agg_path).where(batch_filter)
    hourly = spark.read.format("delta").load(hourly_path).where(batch_filter)
    row_count = fact.count()

    duplicate_fact = fact.groupBy("trip_id").count().where(F.col("count") > 1).count()
    null_critical = fact.where(
        F.col("trip_id").isNull()
        | F.col("pickup_date_key").isNull()
        | F.col("dropoff_date_key").isNull()
        | F.col("pickup_time_key").isNull()
        | F.col("dropoff_time_key").isNull()
        | F.col("pickup_location_id").isNull()
        | F.col("dropoff_location_id").isNull()
    ).count()
    duplicate_dates = dates.groupBy("date_key").count().where(F.col("count") > 1).count()
    duplicate_times = times.groupBy("time_key").count().where(F.col("count") > 1).count()
    duplicate_locations = locations.groupBy("location_id").count().where(F.col("count") > 1).count()

    pickup_date_ri = fact.join(
        dates.select(F.col("date_key").alias("pickup_date_key")), "pickup_date_key", "left_anti"
    ).count()
    dropoff_date_ri = fact.join(
        dates.select(F.col("date_key").alias("dropoff_date_key")), "dropoff_date_key", "left_anti"
    ).count()
    pickup_time_ri = fact.join(
        times.select(F.col("time_key").alias("pickup_time_key")), "pickup_time_key", "left_anti"
    ).count()
    dropoff_time_ri = fact.join(
        times.select(F.col("time_key").alias("dropoff_time_key")), "dropoff_time_key", "left_anti"
    ).count()
    pickup_location_ri = fact.join(
        locations.select(F.col("location_id").alias("pickup_location_id")),
        "pickup_location_id",
        "left_anti",
    ).count()
    dropoff_location_ri = fact.join(
        locations.select(F.col("location_id").alias("dropoff_location_id")),
        "dropoff_location_id",
        "left_anti",
    ).count()

    derived_condition = F.lit(False)
    for column in ("trip_duration_minutes", "average_speed_mph", "fare_per_mile", "tip_percentage"):
        derived_condition = derived_condition | F.isnan(F.col(column)) | F.col(column).isin(
            float("inf"), float("-inf")
        )
    impossible_duration = fact.where(F.col("trip_duration_minutes") < 0).count()
    negative_distance = fact.where(F.col("trip_distance") < 0).count()
    invalid_derived = fact.where(derived_condition).count()
    null_passenger = fact.where(F.col("passenger_count").isNull()).count()
    financial_adjustments = fact.where(F.col("is_financial_adjustment")).count()

    daily_count = daily.agg(F.coalesce(F.sum("trip_count"), F.lit(0)).alias("count")).first()["count"]
    hourly_count = hourly.agg(F.coalesce(F.sum("trip_count"), F.lit(0)).alias("count")).first()["count"]
    location_counts = location_agg.agg(
        F.coalesce(F.sum("pickup_trip_count"), F.lit(0)).alias("pickup"),
        F.coalesce(F.sum("dropoff_trip_count"), F.lit(0)).alias("dropoff"),
    ).first()
    fact_revenue = fact.agg(F.coalesce(F.sum("total_amount"), F.lit(0)).alias("value")).first()["value"]
    daily_revenue = daily.agg(F.coalesce(F.sum("total_revenue"), F.lit(0)).alias("value")).first()["value"]

    metrics = {
        "fact_row_count": row_count,
        "date_dimension_row_count": dates.count(),
        "time_dimension_row_count": times.count(),
        "location_dimension_row_count": locations.count(),
        "daily_aggregation_row_count": daily.count(),
        "location_aggregation_row_count": location_agg.count(),
        "hourly_aggregation_row_count": hourly.count(),
        "empty_fact_count": int(row_count == 0),
        "duplicate_fact_trip_id_count": duplicate_fact,
        "null_critical_key_count": null_critical,
        "duplicate_date_key_count": duplicate_dates,
        "duplicate_time_key_count": duplicate_times,
        "duplicate_location_key_count": duplicate_locations,
        "date_referential_failure_count": pickup_date_ri + dropoff_date_ri,
        "time_referential_failure_count": pickup_time_ri + dropoff_time_ri,
        "location_referential_failure_count": pickup_location_ri + dropoff_location_ri,
        "impossible_duration_count": impossible_duration,
        "negative_distance_count": negative_distance,
        "invalid_derived_metric_count": invalid_derived,
        "daily_reconciliation_failure_count": int(
            daily_count != row_count or abs(float(fact_revenue - daily_revenue)) > 0.01
        ),
        "location_reconciliation_failure_count": int(
            location_counts["pickup"] != row_count or location_counts["dropoff"] != row_count
        ),
        "hourly_reconciliation_failure_count": int(hourly_count != row_count),
        "schema_failure_count": int(bool(missing_required_columns(fact.columns, GOLD_FACT_REQUIRED_COLUMNS))),
        "null_passenger_count": null_passenger,
        "financial_adjustment_count": financial_adjustments,
    }
    result = gold_quality_status(metrics)
    measured_at = utc_now()
    rows = []
    for name, value in sorted(metrics.items()):
        if name in GOLD_QUALITY_CRITICAL_METRICS and float(value) > 0:
            outcome = "FAILED"
        elif name in GOLD_QUALITY_WARNING_METRICS and float(value) > 0:
            outcome = "WARNING"
        else:
            outcome = "PASS"
        rows.append(
            {
                "run_id": run_id,
                "dataset": "gold_phase6",
                "measured_at_utc": measured_at,
                "metric_name": name,
                "metric_value": float(value),
                "threshold": 0.0,
                "outcome": outcome,
            }
        )
    append_gold_quality_rows(spark, quality_path, rows)
    version = schema_version(fact.schema.json())
    if result == "FAILED":
        raise RuntimeError("Gold quality checks failed")
    completed_at = utc_now()
    append_gold_audit(
        spark,
        audit_path,
        GoldAuditRecord(
            run_id, "gold_quality", "gold_phase6", fact_path, quality_path,
            started_at, completed_at, "SUCCEEDED", row_count, result, version,
            int((time.monotonic() - started_clock) * 1_000), None,
        ),
    )
    print({"run_id": run_id, "quality_status": result, **metrics})
except Exception as exc:
    completed_at = utc_now()
    append_gold_audit(
        spark,
        audit_path,
        GoldAuditRecord(
            run_id, "gold_quality", "gold_phase6", fact_path, quality_path,
            started_at, completed_at, "FAILED", row_count, "FAILED", version,
            int((time.monotonic() - started_clock) * 1_000), sanitized_error(exc),
        ),
    )
    raise
