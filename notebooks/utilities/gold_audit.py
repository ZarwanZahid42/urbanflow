# Databricks notebook source
"""Structured audit contracts for UrbanFlow Gold pipelines."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime
from typing import Any


@dataclass(frozen=True)
class GoldAuditRecord:
    run_id: str
    pipeline_name: str
    dataset: str
    source_path: str
    target_path: str
    started_at_utc: datetime
    completed_at_utc: datetime
    status: str
    row_count: int | None
    quality_status: str
    schema_version: str | None
    duration_ms: int
    error: str | None

    def as_row(self) -> dict[str, Any]:
        if self.completed_at_utc < self.started_at_utc:
            raise ValueError("completed_at_utc cannot precede started_at_utc")
        if self.duration_ms < 0:
            raise ValueError("duration_ms cannot be negative")
        return asdict(self)


def append_gold_audit(spark_session: Any, path: str, record: GoldAuditRecord) -> None:
    from pyspark.sql.types import LongType, StringType, StructField, StructType, TimestampType

    schema = StructType(
        [
            StructField("run_id", StringType(), False),
            StructField("pipeline_name", StringType(), False),
            StructField("dataset", StringType(), False),
            StructField("source_path", StringType(), False),
            StructField("target_path", StringType(), False),
            StructField("started_at_utc", TimestampType(), False),
            StructField("completed_at_utc", TimestampType(), False),
            StructField("status", StringType(), False),
            StructField("row_count", LongType(), True),
            StructField("quality_status", StringType(), False),
            StructField("schema_version", StringType(), True),
            StructField("duration_ms", LongType(), False),
            StructField("error", StringType(), True),
        ]
    )
    spark_session.createDataFrame([record.as_row()], schema=schema).write.format("delta").mode(
        "append"
    ).save(path)


def append_gold_quality_rows(spark_session: Any, path: str, rows: list[dict[str, Any]]) -> None:
    from pyspark.sql.types import DoubleType, StringType, StructField, StructType, TimestampType

    schema = StructType(
        [
            StructField("run_id", StringType(), False),
            StructField("dataset", StringType(), False),
            StructField("measured_at_utc", TimestampType(), False),
            StructField("metric_name", StringType(), False),
            StructField("metric_value", DoubleType(), False),
            StructField("threshold", DoubleType(), False),
            StructField("outcome", StringType(), False),
        ]
    )
    spark_session.createDataFrame(rows, schema=schema).write.format("delta").mode("append").save(
        path
    )
