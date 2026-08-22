# Databricks notebook source
# MAGIC %run ../utilities/bronze_common

# COMMAND ----------
# MAGIC %run ../utilities/silver_common

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

bronze_trips = spark.read.format("delta").load(adls_path(YELLOW_DELTA_RELATIVE_PATH)).where(
    yellow_replace_where(source_year, source_month)
)
bronze_zones = spark.read.format("delta").load(adls_path(ZONES_DELTA_RELATIVE_PATH))
fact_path = silver_path(SILVER_FACT_RELATIVE_PATH, ADLS_ROOT)
rejected_trips_path = silver_path(SILVER_REJECTED_TRIPS_RELATIVE_PATH, ADLS_ROOT)
zones_path = silver_path(SILVER_ZONES_RELATIVE_PATH, ADLS_ROOT)
rejected_zones_path = silver_path(SILVER_REJECTED_ZONES_RELATIVE_PATH, ADLS_ROOT)
batch_filter = silver_replace_where(source_year, source_month)
fact = spark.read.format("delta").load(fact_path).where(batch_filter)
rejected_trips = spark.read.format("delta").load(rejected_trips_path).where(batch_filter)
zones = spark.read.format("delta").load(zones_path)
rejected_zones = spark.read.format("delta").load(rejected_zones_path)

bronze_count = bronze_trips.count()
valid_count = fact.count()
rejected_count = rejected_trips.count()
zone_source_count = bronze_zones.count()
zone_valid_count = zones.count()
zone_rejected_count = rejected_zones.count()
if bronze_count != valid_count + rejected_count:
    raise AssertionError("Silver trip counts do not reconcile to Bronze")
if zone_source_count != zone_valid_count + zone_rejected_count:
    raise AssertionError("Silver taxi-zone counts do not reconcile to Bronze")

fact_detail = spark.sql(f"DESCRIBE DETAIL delta.`{fact_path}`").first().asDict()
zone_detail = spark.sql(f"DESCRIBE DETAIL delta.`{zones_path}`").first().asDict()
expected_partitions = ["source_year", "source_month"]
if fact_detail["partitionColumns"] != expected_partitions:
    raise AssertionError(f"Unexpected fact partitions: {fact_detail['partitionColumns']}")
if zone_detail["partitionColumns"]:
    raise AssertionError(f"Taxi-zone dimension must be unpartitioned: {zone_detail['partitionColumns']}")

pickup_failures = fact.join(
    zones.select(F.col("location_id").alias("pickup_location_id")),
    "pickup_location_id",
    "left_anti",
).count()
dropoff_failures = fact.join(
    zones.select(F.col("location_id").alias("dropoff_location_id")),
    "dropoff_location_id",
    "left_anti",
).count()
duplicate_trip_ids = fact.groupBy("trip_id").count().where(F.col("count") > 1).count()
if pickup_failures or dropoff_failures:
    raise AssertionError("Valid Silver trips contain unknown taxi-zone IDs")
if duplicate_trip_ids:
    raise AssertionError("Valid Silver trips contain duplicate deterministic trip IDs")

audit_path = silver_path(SILVER_PIPELINE_AUDIT_RELATIVE_PATH, ADLS_ROOT)
audits = spark.read.format("delta").load(audit_path)
fact_audits = (
    audits.where(
        (F.col("pipeline_name") == "silver_transform_fact_trips")
        & (F.col("dataset") == "fact_trips")
        & F.col("error").isNull()
    )
    .orderBy(F.col("completed_at_utc").desc())
    .limit(required_fact_audits)
    .collect()
)
if len(fact_audits) < required_fact_audits:
    raise AssertionError(f"Expected {required_fact_audits} successful fact audits")
if any(
    row["source_row_count"] != bronze_count
    or row["valid_row_count"] != valid_count
    or row["rejected_row_count"] != rejected_count
    for row in fact_audits
):
    raise AssertionError("Fact audit counts are not stable across idempotency runs")

quality_audit = (
    audits.where((F.col("pipeline_name") == "silver_quality") & F.col("error").isNull())
    .orderBy(F.col("completed_at_utc").desc())
    .first()
)
if quality_audit is None or quality_audit["quality_status"] == "FAILED":
    raise AssertionError("A non-failing Silver quality audit was not found")

rejection_counts = {
    row["rule"]: row["count"]
    for row in rejected_trips.select(F.explode("rejection_rules").alias("rule"))
    .groupBy("rule")
    .count()
    .collect()
}
result = {
    "status": "SUCCEEDED",
    "bronze_trip_count": bronze_count,
    "silver_valid_trip_count": valid_count,
    "silver_rejected_trip_count": rejected_count,
    "bronze_zone_count": zone_source_count,
    "silver_valid_zone_count": zone_valid_count,
    "silver_rejected_zone_count": zone_rejected_count,
    "fact_partition_columns": fact_detail["partitionColumns"],
    "zone_partition_columns": zone_detail["partitionColumns"],
    "pickup_referential_failures": pickup_failures,
    "dropoff_referential_failures": dropoff_failures,
    "duplicate_valid_trip_ids": duplicate_trip_ids,
    "successful_fact_audits_checked": len(fact_audits),
    "quality_status": quality_audit["quality_status"],
    "rejection_counts": rejection_counts,
    "fact_schema_json": fact.schema.json(),
    "zone_schema_json": zones.schema.json(),
}
print(result)
dbutils.notebook.exit(json.dumps(result, default=str))
