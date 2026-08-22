import json
from pathlib import Path

from ingestion.ingestion_audit import AuditRecord, IngestionAudit


def test_audit_record_creation(tmp_path: Path) -> None:
    audit_path = tmp_path / "audit" / "ingestion_audit.jsonl"
    record = AuditRecord(
        run_id="run-1",
        source="nyc_tlc",
        dataset="yellow_tripdata_2026-05",
        source_url="https://example.test/source.parquet",
        started_at="2026-08-22T00:00:00+00:00",
        completed_at="2026-08-22T00:01:00+00:00",
        status="downloaded",
        records_or_bytes=42,
        local_path="data/source.parquet",
    )

    IngestionAudit(audit_path).append(record)

    saved = json.loads(audit_path.read_text(encoding="utf-8").strip())
    assert saved["run_id"] == "run-1"
    assert saved["records_or_bytes"] == 42
    assert saved["error_message"] is None
