# Databricks notebook source
"""Structured Delta audit records for UrbanFlow Databricks pipelines."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime
from typing import Any


@dataclass(frozen=True)
class AuditRecord:
    run_id: str
    pipeline_name: str
    dataset: str
    source_path: str
    target_path: str
    started_at_utc: datetime
    completed_at_utc: datetime
    status: str
    row_count: int | None
    schema_version: str | None
    quality_status: str | None
    error: str | None
    duration_ms: int

    def as_row(self) -> dict[str, Any]:
        if self.completed_at_utc < self.started_at_utc:
            raise ValueError("completed_at_utc cannot precede started_at_utc")
        if self.duration_ms < 0:
            raise ValueError("duration_ms cannot be negative")
        return asdict(self)


def append_audit_record(spark_session: Any, audit_path: str, record: AuditRecord) -> None:
    """Append one credential-free, structured record to the Delta audit dataset."""
    from pyspark.sql.types import (
        LongType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )

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
            StructField("schema_version", StringType(), True),
            StructField("quality_status", StringType(), True),
            StructField("error", StringType(), True),
            StructField("duration_ms", LongType(), False),
        ]
    )
    spark_session.createDataFrame([record.as_row()], schema=schema).write.format("delta").mode(
        "append"
    ).save(audit_path)
