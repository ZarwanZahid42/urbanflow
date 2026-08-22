from datetime import UTC, datetime, timedelta

import pytest

from notebooks.utilities.audit import AuditRecord


def make_record(**overrides):
    started = datetime(2026, 8, 23, tzinfo=UTC)
    values = {
        "run_id": "run-1",
        "pipeline_name": "bronze_ingest_yellow_taxi",
        "dataset": "yellow_taxi",
        "source_path": "abfss://source",
        "target_path": "abfss://target",
        "started_at_utc": started,
        "completed_at_utc": started + timedelta(seconds=2),
        "status": "SUCCEEDED",
        "row_count": 10,
        "schema_version": "sha256:abc",
        "quality_status": "NOT_EVALUATED",
        "error": None,
        "duration_ms": 2_000,
    }
    values.update(overrides)
    return AuditRecord(**values)


def test_audit_record_contains_required_contract():
    row = make_record().as_row()
    assert set(row) == {
        "run_id",
        "pipeline_name",
        "dataset",
        "source_path",
        "target_path",
        "started_at_utc",
        "completed_at_utc",
        "status",
        "row_count",
        "schema_version",
        "quality_status",
        "error",
        "duration_ms",
    }


def test_audit_record_rejects_invalid_timing():
    started = datetime(2026, 8, 23, tzinfo=UTC)
    with pytest.raises(ValueError):
        make_record(completed_at_utc=started - timedelta(seconds=1)).as_row()
    with pytest.raises(ValueError):
        make_record(duration_ms=-1).as_row()
