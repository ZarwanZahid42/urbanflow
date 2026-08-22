"""Environment-backed ADLS Gen2 configuration."""

from __future__ import annotations

import os
import re
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

DEFAULT_STORAGE_ACCOUNT = "urbanflowdata2026"
DEFAULT_FILE_SYSTEM = "urbanflow"
_ACCOUNT_PATTERN = re.compile(r"^[a-z0-9]{3,24}$")
_FILE_SYSTEM_PATTERN = re.compile(r"^[a-z0-9](?:[a-z0-9-]{1,61}[a-z0-9])?$")


@dataclass(frozen=True)
class AzureStorageConfig:
    """Non-secret configuration for the existing UrbanFlow data lake."""

    account_name: str
    file_system: str
    data_dir: Path

    @property
    def account_url(self) -> str:
        return f"https://{self.account_name}.dfs.core.windows.net"

    @property
    def audit_path(self) -> Path:
        return self.data_dir / "audit" / "azure_upload_audit.jsonl"

    @classmethod
    def from_env(cls, env_file: str | Path | None = None) -> AzureStorageConfig:
        load_dotenv(dotenv_path=env_file, override=False)
        account_name = os.getenv(
            "AZURE_STORAGE_ACCOUNT_NAME", DEFAULT_STORAGE_ACCOUNT
        ).strip()
        file_system = os.getenv("AZURE_STORAGE_FILE_SYSTEM", DEFAULT_FILE_SYSTEM).strip()

        if not _ACCOUNT_PATTERN.fullmatch(account_name):
            raise ValueError("AZURE_STORAGE_ACCOUNT_NAME is not a valid storage account name")
        if not _FILE_SYSTEM_PATTERN.fullmatch(file_system):
            raise ValueError("AZURE_STORAGE_FILE_SYSTEM is not a valid filesystem name")

        return cls(
            account_name=account_name,
            file_system=file_system,
            data_dir=Path(os.getenv("DATA_DIR", "data")).expanduser(),
        )
