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

from pyspark.sql import functions as F

dbutils.widgets.text("source_year", "2026")
dbutils.widgets.text("source_month", "5")
source_year = int(dbutils.widgets.get("source_year"))
source_month = int(dbutils.widgets.get("source_month"))
validate_batch(source_year, source_month)

run_id = str(uuid.uuid4())
fact_path = gold_path(GOLD_FACT_RELATIVE_PATH, ADLS_ROOT)
location_path = gold_path(GOLD_LOCATION_RELATIVE_PATH, ADLS_ROOT)
time_path = gold_path(GOLD_TIME_RELATIVE_PATH, ADLS_ROOT)
audit_path = gold_path(GOLD_PIPELINE_AUDIT_RELATIVE_PATH, ADLS_ROOT)
fact = spark.read.format("delta").load(fact_path).where(
    gold_replace_where(source_year, source_month)
)
locations = spark.read.format("delta").load(location_path)
times = spark.read.format("delta").load(time_path)
source_count = fact.count()
if source_count == 0:
    raise ValueError("Gold fact batch contains zero rows")

datasets = (
    ("agg_daily_trips", GOLD_DAILY_RELATIVE_PATH, build_daily_aggregation(fact)),
    (
        "agg_location_trips",
        GOLD_LOCATION_AGG_RELATIVE_PATH,
        build_location_aggregation(fact, locations),
    ),
    ("agg_hourly_trips", GOLD_HOURLY_RELATIVE_PATH, build_hourly_aggregation(fact, times)),
)

for dataset, relative_path, frame in datasets:
    target_path = gold_path(relative_path, ADLS_ROOT)
    started_at = utc_now()
    started_clock = time.monotonic()
    row_count = None
    version = None
    try:
        output = frame.withColumn("gold_run_id", F.lit(run_id)).withColumn(
            "gold_processed_at_utc", F.current_timestamp()
        )
        row_count = output.count()
        if row_count == 0:
            raise ValueError(f"Gold {dataset} contains zero rows")
        version = schema_version(output.schema.json())
        write_gold_partition(output, target_path, source_year, source_month)
        persisted = spark.read.format("delta").load(target_path).where(
            gold_replace_where(source_year, source_month)
        ).count()
        if persisted != row_count:
            raise RuntimeError(f"Persisted Gold {dataset} count mismatch")
        completed_at = utc_now()
        append_gold_audit(
            spark,
            audit_path,
            GoldAuditRecord(
                run_id, "gold_build_aggregations", dataset, fact_path, target_path,
                started_at, completed_at, "SUCCEEDED", row_count, "PASS", version,
                int((time.monotonic() - started_clock) * 1_000), None,
            ),
        )
        print({"run_id": run_id, "dataset": dataset, "status": "SUCCEEDED", "row_count": row_count})
    except Exception as exc:
        completed_at = utc_now()
        append_gold_audit(
            spark,
            audit_path,
            GoldAuditRecord(
                run_id, "gold_build_aggregations", dataset, fact_path, target_path,
                started_at, completed_at, "FAILED", row_count, "FAILED", version,
                int((time.monotonic() - started_clock) * 1_000), sanitized_error(exc),
            ),
        )
        raise
