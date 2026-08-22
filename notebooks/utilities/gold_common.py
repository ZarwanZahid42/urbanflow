# Databricks notebook source
"""Pure-Python contracts for UrbanFlow Gold models and local tests."""

from __future__ import annotations

import math
import re
from datetime import date, timedelta
from typing import Any, Iterable, Mapping

GOLD_FACT_RELATIVE_PATH = "gold/fact_trips"
GOLD_DATE_RELATIVE_PATH = "gold/dim_date"
GOLD_TIME_RELATIVE_PATH = "gold/dim_time"
GOLD_LOCATION_RELATIVE_PATH = "gold/dim_location"
GOLD_DAILY_RELATIVE_PATH = "gold/agg_daily_trips"
GOLD_LOCATION_AGG_RELATIVE_PATH = "gold/agg_location_trips"
GOLD_HOURLY_RELATIVE_PATH = "gold/agg_hourly_trips"
GOLD_PIPELINE_AUDIT_RELATIVE_PATH = "audit/gold_pipeline"
GOLD_QUALITY_AUDIT_RELATIVE_PATH = "audit/gold_quality"

GOLD_FACT_REQUIRED_COLUMNS = (
    "trip_id",
    "pickup_datetime",
    "dropoff_datetime",
    "pickup_date_key",
    "dropoff_date_key",
    "pickup_time_key",
    "dropoff_time_key",
    "pickup_location_id",
    "dropoff_location_id",
    "trip_duration_minutes",
    "average_speed_mph",
    "fare_per_mile",
    "tip_percentage",
    "is_financial_adjustment",
    "source_year",
    "source_month",
    "gold_run_id",
)

GOLD_QUALITY_CRITICAL_METRICS = (
    "empty_fact_count",
    "duplicate_fact_trip_id_count",
    "null_critical_key_count",
    "duplicate_date_key_count",
    "duplicate_time_key_count",
    "duplicate_location_key_count",
    "date_referential_failure_count",
    "time_referential_failure_count",
    "location_referential_failure_count",
    "impossible_duration_count",
    "negative_distance_count",
    "invalid_derived_metric_count",
    "daily_reconciliation_failure_count",
    "location_reconciliation_failure_count",
    "hourly_reconciliation_failure_count",
    "schema_failure_count",
)

GOLD_QUALITY_WARNING_METRICS = (
    "null_passenger_count",
    "financial_adjustment_count",
)


def gold_path(relative_path: str, adls_root: str) -> str:
    cleaned = relative_path.strip().strip("/")
    if not cleaned or "://" in cleaned or ".." in cleaned.split("/"):
        raise ValueError(f"Invalid Gold-relative path: {relative_path!r}")
    return f"{adls_root.rstrip('/')}/{cleaned}"


def gold_replace_where(year: int, month: int) -> str:
    if year < 2009 or year > 9999 or month < 1 or month > 12:
        raise ValueError("Invalid TLC source year/month")
    return f"source_year = {year} AND source_month = {month}"


def date_key(value: date) -> int:
    return value.year * 10_000 + value.month * 100 + value.day


def date_dimension_records(start_date: date, end_date: date) -> list[dict[str, Any]]:
    if end_date < start_date:
        raise ValueError("end_date cannot precede start_date")
    records: list[dict[str, Any]] = []
    current = start_date
    while current <= end_date:
        records.append(
            {
                "date_key": date_key(current),
                "calendar_date": current,
                "year": current.year,
                "quarter": (current.month - 1) // 3 + 1,
                "month": current.month,
                "month_name": current.strftime("%B"),
                "week": current.isocalendar().week,
                "day": current.day,
                "day_of_week": current.isoweekday(),
                "day_name": current.strftime("%A"),
                "is_weekend": current.isoweekday() >= 6,
            }
        )
        current += timedelta(days=1)
    return records


def time_of_day_category(hour: int) -> str:
    if hour < 0 or hour > 23:
        raise ValueError("hour must be between 0 and 23")
    if hour < 6:
        return "Overnight"
    if hour < 12:
        return "Morning"
    if hour < 18:
        return "Afternoon"
    return "Evening"


def time_dimension_records() -> list[dict[str, Any]]:
    return [
        {
            "time_key": hour * 100 + minute,
            "hour": hour,
            "minute": minute,
            "hour_bucket": f"{hour:02d}:00-{hour:02d}:59",
            "am_pm": "AM" if hour < 12 else "PM",
            "time_of_day": time_of_day_category(hour),
        }
        for hour in range(24)
        for minute in range(60)
    ]


def normalize_location_attribute(value: str | None) -> str | None:
    if value is None:
        return None
    normalized = re.sub(r"[^a-z0-9]+", "_", value.strip().lower()).strip("_")
    return normalized or None


def safe_ratio(numerator: float | int | None, denominator: float | int | None) -> float | None:
    if numerator is None or denominator is None:
        return None
    numerator_value = float(numerator)
    denominator_value = float(denominator)
    if not math.isfinite(numerator_value) or not math.isfinite(denominator_value):
        return None
    if denominator_value == 0:
        return None
    result = numerator_value / denominator_value
    return result if math.isfinite(result) else None


def derived_trip_metrics(record: Mapping[str, Any]) -> dict[str, float | None]:
    pickup = record.get("pickup_datetime")
    dropoff = record.get("dropoff_datetime")
    duration = None
    if pickup is not None and dropoff is not None:
        duration = (dropoff - pickup).total_seconds() / 60.0
    distance = record.get("trip_distance")
    speed = safe_ratio(distance, duration / 60.0) if duration is not None and duration > 0 else None
    return {
        "trip_duration_minutes": duration,
        "average_speed_mph": speed,
        "fare_per_mile": safe_ratio(record.get("fare_amount"), distance)
        if distance is not None and distance > 0
        else None,
        "tip_percentage": (
            safe_ratio(record.get("tip_amount"), record.get("fare_amount")) * 100.0
            if safe_ratio(record.get("tip_amount"), record.get("fare_amount")) is not None
            and float(record.get("fare_amount")) > 0
            else None
        ),
    }


def aggregate_daily_records(records: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
    """Small-fixture equivalent of the daily Gold aggregation contract."""
    groups: dict[int, list[Mapping[str, Any]]] = {}
    for record in records:
        groups.setdefault(int(record["pickup_date_key"]), []).append(record)
    output: list[dict[str, Any]] = []
    for key, rows in sorted(groups.items()):
        passengers = [
            float(row["passenger_count"])
            for row in rows
            if row.get("passenger_count") is not None
        ]
        output.append(
            {
                "pickup_date_key": key,
                "trip_count": len(rows),
                "total_revenue": sum(float(row["total_amount"]) for row in rows),
                "total_distance": sum(float(row["trip_distance"]) for row in rows),
                "average_passenger_count": (
                    sum(passengers) / len(passengers) if passengers else None
                ),
                "financial_adjustment_count": sum(
                    bool(row.get("is_financial_adjustment")) for row in rows
                ),
            }
        )
    return output


def gold_quality_status(metrics: Mapping[str, float | int]) -> str:
    if any(float(metrics.get(name, 0)) > 0 for name in GOLD_QUALITY_CRITICAL_METRICS):
        return "FAILED"
    if any(float(metrics.get(name, 0)) > 0 for name in GOLD_QUALITY_WARNING_METRICS):
        return "WARNING"
    return "PASS"


def missing_required_columns(columns: Iterable[str], required: Iterable[str]) -> list[str]:
    return sorted(set(required) - set(columns))
