"""JSON Lines audit persistence for local ingestion runs."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from pathlib import Path


@dataclass(frozen=True)
class AuditRecord:
    """Machine-readable outcome for one source acquisition attempt."""

    run_id: str
    source: str
    dataset: str
    source_url: str
    started_at: str
    completed_at: str
    status: str
    records_or_bytes: int
    local_path: str
    error_message: str | None = None


class IngestionAudit:
    """Append-only local JSON Lines audit store."""

    def __init__(self, path: Path) -> None:
        self.path = path

    def append(self, record: AuditRecord) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.path.open("a", encoding="utf-8", newline="\n") as audit_file:
            json.dump(asdict(record), audit_file, ensure_ascii=False, sort_keys=True)
            audit_file.write("\n")
