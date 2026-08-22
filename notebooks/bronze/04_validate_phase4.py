# Databricks notebook source
# MAGIC %run ../utilities/bronze_common

# COMMAND ----------
import json

from pyspark.sql import functions as F

dbutils.widgets.text("source_year", "2026")
dbutils.widgets.text("source_month", "5")

source_year = int(dbutils.widgets.get("source_year"))
source_month = int(dbutils.widgets.get("source_month"))
validate_batch(source_year, source_month)

yellow_raw = spark.read.parquet(yellow_raw_path(source_year, source_month))
yellow_delta_path = adls_path(YELLOW_DELTA_RELATIVE_PATH)
yellow_delta = spark.read.format("delta").load(yellow_delta_path).where(
    yellow_replace_where(source_year, source_month)
)
zones_raw = spark.read.option("header", True).option("inferSchema", True).csv(
    adls_path(ZONES_RAW_RELATIVE_PATH)
)
zones_delta_path = adls_path(ZONES_DELTA_RELATIVE_PATH)
zones_delta = spark.read.format("delta").load(zones_delta_path)

yellow_raw_count = yellow_raw.count()
yellow_delta_count = yellow_delta.count()
zones_raw_count = zones_raw.count()
zones_delta_count = zones_delta.count()

if yellow_raw_count != yellow_delta_count:
    raise AssertionError(
        f"Yellow retry changed the batch cardinality: raw={yellow_raw_count}, delta={yellow_delta_count}"
    )
if zones_raw_count != zones_delta_count:
    raise AssertionError(
        f"Taxi-zone cardinality mismatch: raw={zones_raw_count}, delta={zones_delta_count}"
    )

yellow_missing_metadata = sorted(TECHNICAL_COLUMNS - set(yellow_delta.columns))
zone_expected_metadata = {SOURCE_FILE_COLUMN, INGESTED_AT_COLUMN, RUN_ID_COLUMN}
zone_missing_metadata = sorted(zone_expected_metadata - set(zones_delta.columns))
if yellow_missing_metadata or zone_missing_metadata:
    raise AssertionError(
        f"Missing metadata columns: yellow={yellow_missing_metadata}, zones={zone_missing_metadata}"
    )

yellow_detail = spark.sql(f"DESCRIBE DETAIL delta.`{yellow_delta_path}`").first().asDict()
zones_detail = spark.sql(f"DESCRIBE DETAIL delta.`{zones_delta_path}`").first().asDict()
expected_partitions = [SOURCE_YEAR_COLUMN, SOURCE_MONTH_COLUMN]
if yellow_detail["partitionColumns"] != expected_partitions:
    raise AssertionError(f"Unexpected Yellow partitions: {yellow_detail['partitionColumns']}")
if zones_detail["partitionColumns"]:
    raise AssertionError(f"Taxi zones must be unpartitioned: {zones_detail['partitionColumns']}")

pipeline_audit = spark.read.format("delta").load(adls_path(PIPELINE_AUDIT_RELATIVE_PATH))
yellow_audits = (
    pipeline_audit.where(
        (F.col("pipeline_name") == "bronze_ingest_yellow_taxi")
        & (F.col("dataset") == "yellow_taxi")
        & (F.col("status") == "SUCCEEDED")
    )
    .orderBy(F.col("completed_at_utc").desc())
    .limit(2)
    .collect()
)
if len(yellow_audits) < 2 or any(row["row_count"] != yellow_delta_count for row in yellow_audits):
    raise AssertionError("Two successful, cardinality-stable Yellow ingestion audits were not found")

quality_audit = (
    pipeline_audit.where(
        (F.col("pipeline_name") == "bronze_quality") & (F.col("status") == "SUCCEEDED")
    )
    .orderBy(F.col("completed_at_utc").desc())
    .first()
)
if quality_audit is None:
    raise AssertionError("A successful Bronze quality audit was not found")

quality_metrics = {
    row["metric_name"]: row["metric_value"]
    for row in spark.read.format("delta")
    .load(adls_path(QUALITY_AUDIT_RELATIVE_PATH))
    .where(F.col("run_id") == quality_audit["run_id"])
    .collect()
}

result = {
    "status": "SUCCEEDED",
    "yellow_raw_count": yellow_raw_count,
    "yellow_delta_count": yellow_delta_count,
    "zones_raw_count": zones_raw_count,
    "zones_delta_count": zones_delta_count,
    "yellow_schema_json": yellow_delta.schema.json(),
    "zones_schema_json": zones_delta.schema.json(),
    "yellow_partition_columns": yellow_detail["partitionColumns"],
    "zones_partition_columns": zones_detail["partitionColumns"],
    "yellow_successful_ingestion_audits_checked": len(yellow_audits),
    "quality_status": quality_audit["quality_status"],
    "quality_metrics": quality_metrics,
}
print(result)
dbutils.notebook.exit(json.dumps(result, default=str))
