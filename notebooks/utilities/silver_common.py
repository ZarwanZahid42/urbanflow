# Databricks notebook source
"""Pure-Python Silver contracts shared by Databricks notebooks and local tests."""

from __future__ import annotations

import hashlib
import math
import re
from decimal import Decimal, InvalidOperation
from typing import Any, Iterable, Mapping

SILVER_FACT_RELATIVE_PATH = "silver/fact_trips"
SILVER_ZONES_RELATIVE_PATH = "silver/dim_taxi_zones"
SILVER_REJECTED_TRIPS_RELATIVE_PATH = "silver/rejected/trips"
SILVER_REJECTED_ZONES_RELATIVE_PATH = "silver/rejected/taxi_zones"
SILVER_PIPELINE_AUDIT_RELATIVE_PATH = "audit/silver_pipeline"
SILVER_QUALITY_AUDIT_RELATIVE_PATH = "audit/silver_quality"

SILVER_SOURCE_YEAR_COLUMN = "source_year"
SILVER_SOURCE_MONTH_COLUMN = "source_month"
MONEY_PRECISION = 18
MONEY_SCALE = 2

TRIP_COLUMN_SPECS = (
    ("VendorID", "vendor_id", "int"),
    ("tpep_pickup_datetime", "pickup_datetime", "timestamp"),
    ("tpep_dropoff_datetime", "dropoff_datetime", "timestamp"),
    ("passenger_count", "passenger_count", "decimal(10,2)"),
    ("trip_distance", "trip_distance", "double"),
    ("RatecodeID", "rate_code_id", "int"),
    ("store_and_fwd_flag", "store_and_forward_flag", "string"),
    ("PULocationID", "pickup_location_id", "int"),
    ("DOLocationID", "dropoff_location_id", "int"),
    ("payment_type", "payment_type", "int"),
    ("fare_amount", "fare_amount", "decimal(18,2)"),
    ("extra", "extra", "decimal(18,2)"),
    ("mta_tax", "mta_tax", "decimal(18,2)"),
    ("tip_amount", "tip_amount", "decimal(18,2)"),
    ("tolls_amount", "tolls_amount", "decimal(18,2)"),
    ("improvement_surcharge", "improvement_surcharge", "decimal(18,2)"),
    ("total_amount", "total_amount", "decimal(18,2)"),
    ("congestion_surcharge", "congestion_surcharge", "decimal(18,2)"),
    ("Airport_fee", "airport_fee", "decimal(18,2)"),
    ("cbd_congestion_fee", "cbd_congestion_fee", "decimal(18,2)"),
    ("_urbanflow_source_file", "source_file", "string"),
    ("_urbanflow_ingested_at_utc", "ingested_at_utc", "timestamp"),
    ("_urbanflow_source_year", "source_year", "int"),
    ("_urbanflow_source_month", "source_month", "int"),
    ("_urbanflow_run_id", "bronze_run_id", "string"),
)

ZONE_COLUMN_SPECS = (
    ("LocationID", "location_id", "int"),
    ("Borough", "borough", "string"),
    ("Zone", "zone", "string"),
    ("service_zone", "service_zone", "string"),
    ("_urbanflow_source_file", "source_file", "string"),
    ("_urbanflow_ingested_at_utc", "ingested_at_utc", "timestamp"),
    ("_urbanflow_run_id", "bronze_run_id", "string"),
)

MONEY_COLUMNS = (
    "fare_amount",
    "extra",
    "mta_tax",
    "tip_amount",
    "tolls_amount",
    "improvement_surcharge",
    "total_amount",
    "congestion_surcharge",
    "airport_fee",
    "cbd_congestion_fee",
)

TRIP_KEY_COLUMNS = tuple(
    target for _, target, _ in TRIP_COLUMN_SPECS if not target.endswith("_utc")
    and target not in {"source_file", "source_year", "source_month", "bronze_run_id"}
)

TRIP_REJECTION_RULE_ORDER = (
    "NULL_PICKUP_TIMESTAMP",
    "NULL_DROPOFF_TIMESTAMP",
    "DROPOFF_BEFORE_PICKUP",
    "NULL_PICKUP_LOCATION",
    "NULL_DROPOFF_LOCATION",
    "NEGATIVE_PASSENGER_COUNT",
    "NULL_TRIP_DISTANCE",
    "INVALID_DISTANCE",
    "NEGATIVE_DISTANCE",
    "NULL_FARE_AMOUNT",
    "NULL_TOTAL_AMOUNT",
    "INVALID_MONETARY_VALUE",
    "INVALID_PICKUP_LOCATION",
    "INVALID_DROPOFF_LOCATION",
    "DUPLICATE_TRIP",
)

ZONE_REJECTION_RULE_ORDER = (
    "NULL_LOCATION_ID",
    "MISSING_BOROUGH",
    "MISSING_ZONE_NAME",
    "DUPLICATE_LOCATION_ID",
)


def silver_path(relative_path: str, adls_root: str) -> str:
    cleaned = relative_path.strip().strip("/")
    if not cleaned or "://" in cleaned or ".." in cleaned.split("/"):
        raise ValueError(f"Invalid Silver-relative path: {relative_path!r}")
    return f"{adls_root.rstrip('/')}/{cleaned}"


def normalize_column_name(name: str) -> str:
    """Normalize common source naming styles to stable snake_case."""
    value = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", name.strip())
    value = re.sub(r"[^A-Za-z0-9]+", "_", value)
    return re.sub(r"_+", "_", value).strip("_").lower()


def silver_replace_where(year: int, month: int) -> str:
    if year < 2009 or year > 9999 or month < 1 or month > 12:
        raise ValueError("Invalid TLC source year/month")
    return f"source_year = {year} AND source_month = {month}"


def _decimal_value(value: Any) -> Decimal | None:
    if value is None:
        return None
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None
    if not parsed.is_finite():
        return None
    maximum = Decimal(10) ** (MONEY_PRECISION - MONEY_SCALE) - Decimal("0.01")
    return parsed if abs(parsed) <= maximum else None


def monetary_value_is_valid(value: Any) -> bool:
    """Null optional amounts are allowed; non-null values must fit Decimal(18,2)."""
    return value is None or _decimal_value(value) is not None


def is_financial_adjustment(record: Mapping[str, Any]) -> bool:
    return any(
        (parsed := _decimal_value(record.get(column))) is not None and parsed < 0
        for column in MONEY_COLUMNS
    )


def classify_trip_record(
    record: Mapping[str, Any],
    valid_location_ids: Iterable[int],
    *,
    duplicate: bool = False,
) -> tuple[str, ...]:
    """Pure equivalent of the Spark rejection contract for small local fixtures."""
    valid_locations = set(valid_location_ids)
    failed: list[str] = []
    pickup = record.get("pickup_datetime")
    dropoff = record.get("dropoff_datetime")
    pickup_location = record.get("pickup_location_id")
    dropoff_location = record.get("dropoff_location_id")

    if pickup is None:
        failed.append("NULL_PICKUP_TIMESTAMP")
    if dropoff is None:
        failed.append("NULL_DROPOFF_TIMESTAMP")
    if pickup is not None and dropoff is not None and dropoff < pickup:
        failed.append("DROPOFF_BEFORE_PICKUP")
    if pickup_location is None:
        failed.append("NULL_PICKUP_LOCATION")
    if dropoff_location is None:
        failed.append("NULL_DROPOFF_LOCATION")
    passenger_count = record.get("passenger_count")
    if passenger_count is not None and passenger_count < 0:
        failed.append("NEGATIVE_PASSENGER_COUNT")
    distance = record.get("trip_distance")
    if distance is None:
        failed.append("NULL_TRIP_DISTANCE")
    elif not math.isfinite(float(distance)):
        failed.append("INVALID_DISTANCE")
    elif distance < 0:
        failed.append("NEGATIVE_DISTANCE")
    if record.get("fare_amount") is None:
        failed.append("NULL_FARE_AMOUNT")
    if record.get("total_amount") is None:
        failed.append("NULL_TOTAL_AMOUNT")
    if any(not monetary_value_is_valid(record.get(column)) for column in MONEY_COLUMNS):
        failed.append("INVALID_MONETARY_VALUE")
    if pickup_location is not None and pickup_location not in valid_locations:
        failed.append("INVALID_PICKUP_LOCATION")
    if dropoff_location is not None and dropoff_location not in valid_locations:
        failed.append("INVALID_DROPOFF_LOCATION")
    if duplicate:
        failed.append("DUPLICATE_TRIP")
    return tuple(rule for rule in TRIP_REJECTION_RULE_ORDER if rule in failed)


def trip_key_payload(record: Mapping[str, Any]) -> str:
    return "\x1f".join(
        "<NULL>" if record.get(column) is None else str(record.get(column))
        for column in TRIP_KEY_COLUMNS
    )


def trip_key(record: Mapping[str, Any]) -> str:
    return hashlib.sha256(trip_key_payload(record).encode("utf-8")).hexdigest()


def silver_quality_status(metrics: Mapping[str, float | int]) -> str:
    source = int(metrics.get("source_row_count", 0))
    valid = int(metrics.get("valid_row_count", 0))
    rejected = int(metrics.get("rejected_row_count", 0))
    if source <= 0 or valid <= 0 or source != valid + rejected:
        return "FAILED"
    rejection_rate = rejected / source
    if rejection_rate > 0.20:
        return "FAILED"
    warning_names = (
        "rejected_row_count",
        "duplicate_count",
        "invalid_timestamp_count",
        "invalid_location_count",
        "null_passenger_count",
        "negative_fare_amount_count",
        "negative_total_amount_count",
        "referential_integrity_failure_count",
    )
    return "WARNING" if any(float(metrics.get(name, 0)) > 0 for name in warning_names) else "PASS"
