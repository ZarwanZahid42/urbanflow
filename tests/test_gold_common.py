from datetime import UTC, date, datetime, timedelta

import pytest

from notebooks.utilities.gold_common import (
    GOLD_FACT_RELATIVE_PATH,
    aggregate_daily_records,
    date_dimension_records,
    date_key,
    derived_trip_metrics,
    gold_path,
    gold_quality_status,
    gold_replace_where,
    missing_required_columns,
    normalize_location_attribute,
    safe_ratio,
    time_dimension_records,
    time_of_day_category,
)


def test_gold_path_and_monthly_predicate():
    root = "abfss://urbanflow@example.dfs.core.windows.net"
    assert gold_path(GOLD_FACT_RELATIVE_PATH, root) == f"{root}/gold/fact_trips"
    assert gold_replace_where(2026, 5) == "source_year = 2026 AND source_month = 5"
    with pytest.raises(ValueError):
        gold_path("../bronze", root)


def test_date_dimension_contract_is_deterministic_and_inclusive():
    records = date_dimension_records(date(2026, 5, 30), date(2026, 6, 1))
    assert [record["date_key"] for record in records] == [20260530, 20260531, 20260601]
    assert records[1]["day_name"] == "Sunday"
    assert records[1]["is_weekend"] is True
    assert date_key(date(2026, 5, 1)) == 20260501


def test_date_dimension_rejects_reversed_bounds():
    with pytest.raises(ValueError):
        date_dimension_records(date(2026, 6, 1), date(2026, 5, 1))


def test_time_dimension_has_every_unique_minute_and_categories():
    records = time_dimension_records()
    assert len(records) == 1_440
    assert len({record["time_key"] for record in records}) == 1_440
    assert records[0] == {
        "time_key": 0,
        "hour": 0,
        "minute": 0,
        "hour_bucket": "00:00-00:59",
        "am_pm": "AM",
        "time_of_day": "Overnight",
    }
    assert records[-1]["time_key"] == 2359
    assert time_of_day_category(6) == "Morning"
    assert time_of_day_category(12) == "Afternoon"
    assert time_of_day_category(18) == "Evening"


def test_location_normalization_is_stable():
    assert normalize_location_attribute("  East Elmhurst / JFK  ") == "east_elmhurst_jfk"
    assert normalize_location_attribute(None) is None


def test_safe_ratio_never_returns_infinity_or_divides_by_zero():
    assert safe_ratio(10, 2) == 5.0
    assert safe_ratio(10, 0) is None
    assert safe_ratio(float("inf"), 2) is None
    assert safe_ratio(None, 2) is None


def test_derived_metrics_preserve_valid_math_and_unknowns():
    pickup = datetime(2026, 5, 1, 12, tzinfo=UTC)
    metrics = derived_trip_metrics(
        {
            "pickup_datetime": pickup,
            "dropoff_datetime": pickup + timedelta(minutes=30),
            "trip_distance": 15,
            "fare_amount": 30,
            "tip_amount": 6,
        }
    )
    assert metrics == {
        "trip_duration_minutes": 30.0,
        "average_speed_mph": 30.0,
        "fare_per_mile": 2.0,
        "tip_percentage": 20.0,
    }
    assert derived_trip_metrics(
        {
            "pickup_datetime": pickup,
            "dropoff_datetime": pickup,
            "trip_distance": 0,
            "fare_amount": 0,
            "tip_amount": 0,
        }
    )["average_speed_mph"] is None


def test_daily_aggregation_preserves_adjustments_and_ignores_null_passengers_in_average():
    rows = [
        {
            "pickup_date_key": 20260501,
            "total_amount": 20,
            "trip_distance": 4,
            "passenger_count": 2,
            "is_financial_adjustment": False,
        },
        {
            "pickup_date_key": 20260501,
            "total_amount": -5,
            "trip_distance": 1,
            "passenger_count": None,
            "is_financial_adjustment": True,
        },
    ]
    assert aggregate_daily_records(rows) == [
        {
            "pickup_date_key": 20260501,
            "trip_count": 2,
            "total_revenue": 15.0,
            "total_distance": 5.0,
            "average_passenger_count": 2.0,
            "financial_adjustment_count": 1,
        }
    ]


def test_gold_quality_status_separates_critical_failures_from_observations():
    assert gold_quality_status({}) == "PASS"
    assert gold_quality_status({"null_passenger_count": 10}) == "WARNING"
    assert gold_quality_status({"financial_adjustment_count": 1}) == "WARNING"
    assert gold_quality_status({"duplicate_fact_trip_id_count": 1}) == "FAILED"


def test_schema_contract_reports_missing_columns():
    assert missing_required_columns(["trip_id", "source_year"], ["trip_id", "source_month"]) == [
        "source_month"
    ]
