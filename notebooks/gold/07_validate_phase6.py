# Databricks notebook source
# MAGIC %run ../utilities/bronze_common

# COMMAND ----------
# MAGIC %run ../utilities/silver_common

# COMMAND ----------
# MAGIC %run ../utilities/gold_common

# COMMAND ----------
import json

from pyspark.sql import functions as F

dbutils.widgets.text("source_year", "2026")
dbutils.widgets.text("source_month", "5")
dbutils.widgets.text("required_fact_audits", "1")
source_year = int(dbutils.widgets.get("source_year"))
source_month = int(dbutils.widgets.get("source_month"))
required_fact_audits = int(dbutils.widgets.get("required_fact_audits"))
validate_batch(source_year, source_month)

silver_path_value = silver_path(SILVER_FACT_RELATIVE_PATH, ADLS_ROOT)
fact_path = gold_path(GOLD_FACT_RELATIVE_PATH, ADLS_ROOT)
date_path = gold_path(GOLD_DATE_RELATIVE_PATH, ADLS_ROOT)
time_path = gold_path(GOLD_TIME_RELATIVE_PATH, ADLS_ROOT)
location_path = gold_path(GOLD_LOCATION_RELATIVE_PATH, ADLS_ROOT)
daily_path = gold_path(GOLD_DAILY_RELATIVE_PATH, ADLS_ROOT)
location_agg_path = gold_path(GOLD_LOCATION_AGG_RELATIVE_PATH, ADLS_ROOT)
hourly_path = gold_path(GOLD_HOURLY_RELATIVE_PATH, ADLS_ROOT)
batch_filter = gold_replace_where(source_year, source_month)

silver_fact = spark.read.format("delta").load(silver_path_value).where(batch_filter)
fact = spark.read.format("delta").load(fact_path).where(batch_filter)
dates = spark.read.format("delta").load(date_path)
times = spark.read.format("delta").load(time_path)
locations = spark.read.format("delta").load(location_path)
daily = spark.read.format("delta").load(daily_path).where(batch_filter)
location_agg = spark.read.format("delta").load(location_agg_path).where(batch_filter)
hourly = spark.read.format("delta").load(hourly_path).where(batch_filter)

silver_count = silver_fact.count()
fact_count = fact.count()
if silver_count != fact_count:
    raise AssertionError(f"Silver/Gold fact mismatch: silver={silver_count}, gold={fact_count}")
if fact.select("trip_id").distinct().count() != fact_count:
    raise AssertionError("Gold fact trip IDs are not unique")

dimension_counts = {
    "dim_date": dates.count(),
    "dim_time": times.count(),
    "dim_location": locations.count(),
}
if not all(dimension_counts.values()) or dimension_counts["dim_time"] != 1_440:
    raise AssertionError(f"Invalid Gold dimension counts: {dimension_counts}")

date_failures = fact.join(
    dates.select(F.col("date_key").alias("pickup_date_key")), "pickup_date_key", "left_anti"
).count() + fact.join(
    dates.select(F.col("date_key").alias("dropoff_date_key")), "dropoff_date_key", "left_anti"
).count()
time_failures = fact.join(
    times.select(F.col("time_key").alias("pickup_time_key")), "pickup_time_key", "left_anti"
).count() + fact.join(
    times.select(F.col("time_key").alias("dropoff_time_key")), "dropoff_time_key", "left_anti"
).count()
location_failures = fact.join(
    locations.select(F.col("location_id").alias("pickup_location_id")),
    "pickup_location_id",
    "left_anti",
).count() + fact.join(
    locations.select(F.col("location_id").alias("dropoff_location_id")),
    "dropoff_location_id",
    "left_anti",
).count()
if date_failures or time_failures or location_failures:
    raise AssertionError("Gold fact contains dimension referential-integrity failures")

daily_trip_count = daily.agg(F.sum("trip_count").alias("value")).first()["value"]
hourly_trip_count = hourly.agg(F.sum("trip_count").alias("value")).first()["value"]
location_totals = location_agg.agg(
    F.sum("pickup_trip_count").alias("pickup"),
    F.sum("dropoff_trip_count").alias("dropoff"),
).first()
if daily_trip_count != fact_count or hourly_trip_count != fact_count:
    raise AssertionError("Gold daily/hourly aggregations do not reconcile to fact")
if location_totals["pickup"] != fact_count or location_totals["dropoff"] != fact_count:
    raise AssertionError("Gold location aggregation does not reconcile to fact")

fact_detail = spark.sql(f"DESCRIBE DETAIL delta.`{fact_path}`").first().asDict()
date_detail = spark.sql(f"DESCRIBE DETAIL delta.`{date_path}`").first().asDict()
if fact_detail["partitionColumns"] != ["source_year", "source_month"]:
    raise AssertionError(f"Unexpected Gold fact partitions: {fact_detail['partitionColumns']}")
if date_detail["partitionColumns"]:
    raise AssertionError("Gold date dimension must be unpartitioned")

audit_path = gold_path(GOLD_PIPELINE_AUDIT_RELATIVE_PATH, ADLS_ROOT)
audits = spark.read.format("delta").load(audit_path)
fact_audits = (
    audits.where(
        (F.col("pipeline_name") == "gold_build_fact_trips")
        & (F.col("dataset") == "fact_trips")
        & (F.col("status") == "SUCCEEDED")
        & F.col("error").isNull()
    )
    .orderBy(F.col("completed_at_utc").desc())
    .limit(required_fact_audits)
    .collect()
)
if len(fact_audits) < required_fact_audits:
    raise AssertionError(f"Expected {required_fact_audits} successful Gold fact audits")
if any(row["row_count"] != fact_count for row in fact_audits):
    raise AssertionError("Gold fact audit counts are not stable")

quality_audit = (
    audits.where(
        (F.col("pipeline_name") == "gold_quality")
        & (F.col("status") == "SUCCEEDED")
        & F.col("error").isNull()
    )
    .orderBy(F.col("completed_at_utc").desc())
    .first()
)
if quality_audit is None or quality_audit["quality_status"] == "FAILED":
    raise AssertionError("A non-failing Gold quality audit was not found")

quality_metrics_path = gold_path(GOLD_QUALITY_AUDIT_RELATIVE_PATH, ADLS_ROOT)
quality_rows = (
    spark.read.format("delta")
    .load(quality_metrics_path)
    .where(F.col("run_id") == quality_audit["run_id"])
    .collect()
)
quality_metrics = {row["metric_name"]: row["metric_value"] for row in quality_rows}
if any(row["outcome"] == "FAILED" for row in quality_rows):
    raise AssertionError("Latest Gold quality metrics contain a failed outcome")

result = {
    "status": "SUCCEEDED",
    "silver_fact_row_count": silver_count,
    "gold_fact_row_count": fact_count,
    **dimension_counts,
    "agg_daily_trips": daily.count(),
    "agg_location_trips": location_agg.count(),
    "agg_hourly_trips": hourly.count(),
    "daily_reconciled_trip_count": daily_trip_count,
    "pickup_reconciled_trip_count": location_totals["pickup"],
    "dropoff_reconciled_trip_count": location_totals["dropoff"],
    "hourly_reconciled_trip_count": hourly_trip_count,
    "date_referential_failures": date_failures,
    "time_referential_failures": time_failures,
    "location_referential_failures": location_failures,
    "duplicate_fact_trip_ids": 0,
    "fact_partition_columns": fact_detail["partitionColumns"],
    "successful_fact_audits_checked": len(fact_audits),
    "quality_status": quality_audit["quality_status"],
    "quality_metrics": quality_metrics,
    "fact_schema_json": fact.schema.json(),
}
print(result)
dbutils.notebook.exit(json.dumps(result, default=str))
