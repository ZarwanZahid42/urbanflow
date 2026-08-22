# Databricks notebook source
"""Shared Bronze configuration and helpers.

This file deliberately imports no PySpark modules at import time so its contracts can
be unit tested locally. Spark-specific functions import PySpark only when Databricks
executes them.
"""

from __future__ import annotations

import hashlib
from datetime import UTC, datetime
from typing import Any, Iterable

STORAGE_ACCOUNT = "urbanflowdata2026"
FILESYSTEM = "urbanflow"
ADLS_ROOT = f"abfss://{FILESYSTEM}@{STORAGE_ACCOUNT}.dfs.core.windows.net"

YELLOW_RAW_TEMPLATE = "bronze/tlc/yellow/year={year}/month={month:02d}/source.parquet"
ZONES_RAW_RELATIVE_PATH = "bronze/reference/taxi_zones/taxi_zone_lookup.csv"
YELLOW_DELTA_RELATIVE_PATH = "bronze/delta/yellow_taxi"
ZONES_DELTA_RELATIVE_PATH = "bronze/delta/taxi_zones"
PIPELINE_AUDIT_RELATIVE_PATH = "audit/bronze_pipeline"
QUALITY_AUDIT_RELATIVE_PATH = "audit/bronze_quality"

SOURCE_FILE_COLUMN = "_urbanflow_source_file"
FILE_PATH_METADATA_COLUMN = "_metadata.file_path"
INGESTED_AT_COLUMN = "_urbanflow_ingested_at_utc"
RUN_ID_COLUMN = "_urbanflow_run_id"
SOURCE_YEAR_COLUMN = "_urbanflow_source_year"
SOURCE_MONTH_COLUMN = "_urbanflow_source_month"
TECHNICAL_COLUMNS = {
    SOURCE_FILE_COLUMN,
    INGESTED_AT_COLUMN,
    RUN_ID_COLUMN,
    SOURCE_YEAR_COLUMN,
    SOURCE_MONTH_COLUMN,
}

YELLOW_REQUIRED_COLUMNS = (
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "PULocationID",
    "DOLocationID",
    "passenger_count",
    "fare_amount",
    "total_amount",
)
ZONE_REQUIRED_COLUMNS = ("LocationID", "Borough", "Zone", "service_zone")


def adls_path(relative_path: str) -> str:
    """Return a canonical ABFSS path below the configured filesystem."""
    cleaned = relative_path.strip().strip("/")
    if not cleaned or "://" in cleaned or ".." in cleaned.split("/"):
        raise ValueError(f"Invalid ADLS-relative path: {relative_path!r}")
    return f"{ADLS_ROOT}/{cleaned}"


def yellow_raw_path(year: int, month: int) -> str:
    validate_batch(year, month)
    return adls_path(YELLOW_RAW_TEMPLATE.format(year=year, month=month))


def validate_batch(year: int, month: int) -> None:
    if year < 2009 or year > 9999:
        raise ValueError("year must be between 2009 and 9999")
    if month < 1 or month > 12:
        raise ValueError("month must be between 1 and 12")


def utc_now() -> datetime:
    return datetime.now(UTC)


def schema_version(schema_json: str) -> str:
    """Create a stable, content-addressed schema identifier."""
    return f"sha256:{hashlib.sha256(schema_json.encode('utf-8')).hexdigest()}"


def missing_columns(columns: Iterable[str], required: Iterable[str]) -> list[str]:
    present = set(columns)
    return sorted(set(required) - present)


def yellow_replace_where(year: int, month: int) -> str:
    validate_batch(year, month)
    return f"{SOURCE_YEAR_COLUMN} = {year} AND {SOURCE_MONTH_COLUMN} = {month}"


def quality_status(metrics: dict[str, int], warning_metrics: Iterable[str]) -> str:
    """Return WARNING when any zero-tolerance report-only metric is nonzero."""
    return "WARNING" if any(metrics.get(name, 0) > 0 for name in warning_metrics) else "PASSED"


def sanitized_error(error: BaseException | str | None, limit: int = 2_000) -> str | None:
    if error is None:
        return None
    message = " ".join(str(error).split())
    if message:
        return message[:limit]
    return error.__class__.__name__ if isinstance(error, BaseException) else None


def add_ingestion_metadata(
    dataframe: Any,
    *,
    run_id: str,
    year: int | None = None,
    month: int | None = None,
    spark_functions: Any | None = None,
) -> Any:
    """Add Unity Catalog-compatible traceability without altering source columns."""
    if spark_functions is None:
        from pyspark.sql import functions as spark_functions

    result = (
        dataframe.withColumn(SOURCE_FILE_COLUMN, spark_functions.col(FILE_PATH_METADATA_COLUMN))
        .withColumn(INGESTED_AT_COLUMN, spark_functions.current_timestamp())
        .withColumn(RUN_ID_COLUMN, spark_functions.lit(run_id))
    )
    if year is not None or month is not None:
        if year is None or month is None:
            raise ValueError("year and month must be supplied together")
        validate_batch(year, month)
        result = result.withColumn(
            SOURCE_YEAR_COLUMN, spark_functions.lit(year)
        ).withColumn(SOURCE_MONTH_COLUMN, spark_functions.lit(month))
    return result


def write_yellow_delta(dataframe: Any, target_path: str, year: int, month: int) -> None:
    """Atomically replace one Yellow Taxi batch and retain other partitions."""
    (
        dataframe.write.format("delta")
        .mode("overwrite")
        .option("replaceWhere", yellow_replace_where(year, month))
        .option("mergeSchema", "true")
        .partitionBy(SOURCE_YEAR_COLUMN, SOURCE_MONTH_COLUMN)
        .save(target_path)
    )


def write_reference_delta(dataframe: Any, target_path: str) -> None:
    """Replace the complete, small reference snapshot without partitioning it."""
    dataframe.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(
        target_path
    )
