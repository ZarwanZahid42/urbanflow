"""Append-only audit records for Azure data lake uploads."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from pathlib import Path


@dataclass(frozen=True)
class AzureUploadAuditRecord:
    run_id: str
    source: str
    local_path: str
    remote_path: str
    storage_account: str
    filesystem: str
    started_at: str
    completed_at: str
    status: str
    bytes_uploaded: int
    error_message: str | None = None


class AzureUploadAudit:
    """Persist upload outcomes as JSON Lines without credential material."""

    def __init__(self, path: Path) -> None:
        self.path = path

    def append(self, record: AzureUploadAuditRecord) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.path.open("a", encoding="utf-8", newline="\n") as audit_file:
            json.dump(asdict(record), audit_file, ensure_ascii=False, sort_keys=True)
            audit_file.write("\n")
