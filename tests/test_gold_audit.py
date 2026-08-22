from datetime import UTC, datetime, timedelta

import pytest

from notebooks.utilities.gold_audit import GoldAuditRecord


def audit_record(**overrides):
    started = datetime(2026, 8, 23, tzinfo=UTC)
    values = {
        "run_id": "gold-run-1",
        "pipeline_name": "gold_build_fact_trips",
        "dataset": "fact_trips",
        "source_path": "abfss://silver",
        "target_path": "abfss://gold",
        "started_at_utc": started,
        "completed_at_utc": started + timedelta(seconds=4),
        "status": "SUCCEEDED",
        "row_count": 10,
        "quality_status": "PASS",
        "schema_version": "sha256:abc",
        "duration_ms": 4_000,
        "error": None,
    }
    values.update(overrides)
    return GoldAuditRecord(**values)


def test_gold_audit_has_required_contract():
    assert set(audit_record().as_row()) == {
        "run_id",
        "pipeline_name",
        "dataset",
        "source_path",
        "target_path",
        "started_at_utc",
        "completed_at_utc",
        "status",
        "row_count",
        "quality_status",
        "schema_version",
        "duration_ms",
        "error",
    }


def test_gold_audit_rejects_invalid_duration_and_timestamps():
    started = datetime(2026, 8, 23, tzinfo=UTC)
    with pytest.raises(ValueError):
        audit_record(completed_at_utc=started - timedelta(seconds=1)).as_row()
    with pytest.raises(ValueError):
        audit_record(duration_ms=-1).as_row()
