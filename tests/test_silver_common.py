from datetime import UTC, datetime, timedelta

from notebooks.utilities.silver_common import (
    SILVER_FACT_RELATIVE_PATH,
    classify_trip_record,
    is_financial_adjustment,
    monetary_value_is_valid,
    normalize_column_name,
    silver_path,
    silver_quality_status,
    silver_replace_where,
    trip_key,
)


def valid_trip():
    pickup = datetime(2026, 5, 1, 12, tzinfo=UTC)
    return {
        "vendor_id": 2,
        "pickup_datetime": pickup,
        "dropoff_datetime": pickup + timedelta(minutes=12),
        "passenger_count": 1,
        "trip_distance": 2.5,
        "rate_code_id": 1,
        "store_and_forward_flag": "N",
        "pickup_location_id": 100,
        "dropoff_location_id": 200,
        "payment_type": 1,
        "fare_amount": "14.50",
        "extra": "1.00",
        "mta_tax": "0.50",
        "tip_amount": "3.00",
        "tolls_amount": "0.00",
        "improvement_surcharge": "1.00",
        "total_amount": "20.00",
        "congestion_surcharge": "0.00",
        "airport_fee": "0.00",
        "cbd_congestion_fee": "0.00",
    }


def test_column_normalization_handles_tlc_names():
    assert normalize_column_name("VendorID") == "vendor_id"
    assert normalize_column_name("PULocationID") == "pulocation_id"
    assert normalize_column_name("store_and_fwd_flag") == "store_and_fwd_flag"


def test_silver_path_and_batch_predicate_are_incremental():
    root = "abfss://urbanflow@example.dfs.core.windows.net"
    assert silver_path(SILVER_FACT_RELATIVE_PATH, root) == f"{root}/silver/fact_trips"
    assert silver_replace_where(2027, 11) == "source_year = 2027 AND source_month = 11"


def test_valid_trip_has_no_rejection_rules():
    assert classify_trip_record(valid_trip(), {100, 200}) == ()


def test_rejection_classification_retains_multiple_structured_rules():
    record = valid_trip()
    record.update(
        {
            "dropoff_datetime": record["pickup_datetime"] - timedelta(seconds=1),
            "passenger_count": -1,
            "trip_distance": -0.5,
            "pickup_location_id": 999,
        }
    )
    assert classify_trip_record(record, {100, 200}, duplicate=True) == (
        "DROPOFF_BEFORE_PICKUP",
        "NEGATIVE_PASSENGER_COUNT",
        "NEGATIVE_DISTANCE",
        "INVALID_PICKUP_LOCATION",
        "DUPLICATE_TRIP",
    )


def test_null_passenger_is_preserved_as_unknown():
    record = valid_trip()
    record["passenger_count"] = None
    assert classify_trip_record(record, {100, 200}) == ()


def test_negative_money_is_preserved_as_financial_adjustment():
    record = valid_trip()
    record["fare_amount"] = "-14.50"
    record["total_amount"] = "-20.00"
    assert monetary_value_is_valid(record["fare_amount"])
    assert is_financial_adjustment(record)
    assert classify_trip_record(record, {100, 200}) == ()


def test_nonfinite_or_overflow_money_is_invalid():
    assert not monetary_value_is_valid("NaN")
    assert not monetary_value_is_valid("Infinity")
    assert not monetary_value_is_valid("10000000000000000.00")


def test_trip_key_is_stable_and_changes_with_business_values():
    first = valid_trip()
    second = dict(first)
    assert trip_key(first) == trip_key(second)
    second["trip_distance"] = 2.6
    assert trip_key(first) != trip_key(second)


def test_quality_classification_thresholds():
    assert (
        silver_quality_status(
            {"source_row_count": 100, "valid_row_count": 100, "rejected_row_count": 0}
        )
        == "PASS"
    )
    assert (
        silver_quality_status(
            {
                "source_row_count": 100,
                "valid_row_count": 99,
                "rejected_row_count": 1,
            }
        )
        == "WARNING"
    )
    assert (
        silver_quality_status(
            {"source_row_count": 100, "valid_row_count": 80, "rejected_row_count": 10}
        )
        == "FAILED"
    )
    assert (
        silver_quality_status(
            {"source_row_count": 100, "valid_row_count": 79, "rejected_row_count": 21}
        )
        == "FAILED"
    )
