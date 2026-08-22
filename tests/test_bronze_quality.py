from notebooks.utilities.quality import quality_rows


def test_quality_rows_are_structured_and_report_only():
    rows = quality_rows("run-1", "yellow_taxi", {"row_count": 5, "negative_fare": 2})
    indexed = {row["metric_name"]: row for row in rows}
    assert indexed["row_count"]["threshold"] is None
    assert indexed["row_count"]["outcome"] == "PASSED"
    assert indexed["negative_fare"]["threshold"] == 0
    assert indexed["negative_fare"]["outcome"] == "WARNING"
