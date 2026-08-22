# Databricks notebook source
"""Report-only Bronze quality rules and Spark evaluation helpers."""

from __future__ import annotations

from typing import Any

YELLOW_WARNING_METRICS = (
    "null_pickup_timestamp_count",
    "null_dropoff_timestamp_count",
    "null_pickup_location_count",
    "null_dropoff_location_count",
    "duplicate_row_count",
    "invalid_timestamp_count",
    "negative_passenger_count",
    "negative_fare_amount_count",
    "negative_total_amount_count",
)


def evaluate_yellow_quality(dataframe: Any, source_columns: list[str]) -> dict[str, int]:
    """Compute all requested anomaly counts without filtering or changing Bronze."""
    from pyspark.sql import functions as F

    aggregate = dataframe.agg(
        F.count(F.lit(1)).alias("row_count"),
        F.sum(F.when(F.col("tpep_pickup_datetime").isNull(), 1).otherwise(0)).alias(
            "null_pickup_timestamp_count"
        ),
        F.sum(F.when(F.col("tpep_dropoff_datetime").isNull(), 1).otherwise(0)).alias(
            "null_dropoff_timestamp_count"
        ),
        F.sum(F.when(F.col("PULocationID").isNull(), 1).otherwise(0)).alias(
            "null_pickup_location_count"
        ),
        F.sum(F.when(F.col("DOLocationID").isNull(), 1).otherwise(0)).alias(
            "null_dropoff_location_count"
        ),
        F.sum(
            F.when(
                F.col("tpep_dropoff_datetime") < F.col("tpep_pickup_datetime"), 1
            ).otherwise(0)
        ).alias("invalid_timestamp_count"),
        F.sum(F.when(F.col("passenger_count") < 0, 1).otherwise(0)).alias(
            "negative_passenger_count"
        ),
        F.sum(F.when(F.col("fare_amount") < 0, 1).otherwise(0)).alias(
            "negative_fare_amount_count"
        ),
        F.sum(F.when(F.col("total_amount") < 0, 1).otherwise(0)).alias(
            "negative_total_amount_count"
        ),
    ).first()
    metrics = {name: int(value or 0) for name, value in aggregate.asDict().items()}

    duplicate_excess = (
        dataframe.groupBy(*source_columns)
        .count()
        .where(F.col("count") > 1)
        .agg(F.coalesce(F.sum(F.col("count") - 1), F.lit(0)).alias("duplicate_row_count"))
        .first()["duplicate_row_count"]
    )
    metrics["duplicate_row_count"] = int(duplicate_excess)
    return metrics


def quality_rows(run_id: str, dataset: str, metrics: dict[str, int]) -> list[dict[str, Any]]:
    """Flatten metrics for append-only Delta storage and simple querying."""
    return [
        {
            "run_id": run_id,
            "dataset": dataset,
            "metric_name": name,
            "metric_value": int(value),
            "threshold": 0 if name != "row_count" else None,
            "outcome": "WARNING" if name != "row_count" and value > 0 else "PASSED",
        }
        for name, value in sorted(metrics.items())
    ]
