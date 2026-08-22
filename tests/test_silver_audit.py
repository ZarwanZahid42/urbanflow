from datetime import UTC, datetime, timedelta

import pytest

from notebooks.utilities.silver_audit import SilverAuditRecord


def audit_record(**overrides):
    started = datetime(2026, 8, 23, tzinfo=UTC)
    values = {
        "run_id": "silver-run-1",
        "pipeline_name": "silver_transform_fact_trips",
        "dataset": "fact_trips",
        "source_path": "abfss://bronze",
        "target_path": "abfss://silver",
        "started_at_utc": started,
        "completed_at_utc": started + timedelta(seconds=5),
        "source_row_count": 10,
        "valid_row_count": 9,
        "rejected_row_count": 1,
        "quality_status": "WARNING",
        "schema_version": "sha256:abc",
        "duration_ms": 5_000,
        "error": None,
    }
    values.update(overrides)
    return SilverAuditRecord(**values)


def test_silver_audit_has_required_contract():
    assert set(audit_record().as_row()) == {
        "run_id",
        "pipeline_name",
        "dataset",
        "source_path",
        "target_path",
        "started_at_utc",
        "completed_at_utc",
        "source_row_count",
        "valid_row_count",
        "rejected_row_count",
        "quality_status",
        "schema_version",
        "duration_ms",
        "error",
    }


def test_silver_audit_rejects_invalid_duration_and_timestamps():
    started = datetime(2026, 8, 23, tzinfo=UTC)
    with pytest.raises(ValueError):
        audit_record(completed_at_utc=started - timedelta(seconds=1)).as_row()
    with pytest.raises(ValueError):
        audit_record(duration_ms=-1).as_row()
