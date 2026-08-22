"""Azure identity-based ADLS Gen2 file and directory uploads."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any
from uuid import uuid4

from azure.core.exceptions import AzureError, ResourceExistsError, ResourceNotFoundError
from azure.identity import DefaultAzureCredential
from azure.storage.filedatalake import DataLakeServiceClient

from .config import AzureStorageConfig

logger = logging.getLogger(__name__)
UPLOAD_CHUNK_SIZE = 4 * 1024 * 1024


class AzureStorageOperationError(RuntimeError):
    """A sanitized, path-specific ADLS operation failure."""


class RemoteFileConflictError(AzureStorageOperationError):
    """The remote path exists with a different size and overwrite is disabled."""


@dataclass(frozen=True)
class UploadResult:
    local_path: Path
    remote_path: str
    status: str
    local_size: int
    remote_size: int
    bytes_uploaded: int


def normalize_remote_path(remote_path: str) -> str:
    """Validate and normalize a filesystem-relative ADLS path."""

    if not remote_path or "\\" in remote_path or remote_path.startswith("/"):
        raise ValueError("Remote path must be a non-empty relative POSIX path")
    path = PurePosixPath(remote_path)
    if any(part in {"", ".", ".."} for part in path.parts):
        raise ValueError("Remote path contains an invalid segment")
    return path.as_posix()


def remote_path_for(local_path: Path, data_dir: Path) -> str:
    """Map a local Bronze file to the same logical path in ADLS."""

    bronze_root = (data_dir / "bronze").resolve()
    try:
        relative_path = local_path.resolve().relative_to(bronze_root)
    except ValueError as exc:
        raise ValueError(f"Local path is outside the Bronze root: {local_path}") from exc
    return normalize_remote_path(PurePosixPath("bronze", *relative_path.parts).as_posix())


def _property_size(properties: Any) -> int:
    size = getattr(properties, "size", None)
    if size is None and isinstance(properties, dict):
        size = properties.get("size") or properties.get("content_length")
    if size is None:
        raise AzureStorageOperationError("ADLS file properties did not include a file size")
    return int(size)


class ADLSClient:
    """Upload existing local files through DefaultAzureCredential only."""

    def __init__(
        self,
        config: AzureStorageConfig,
        *,
        credential: Any | None = None,
        service_client: Any | None = None,
    ) -> None:
        self.config = config
        self.credential = credential or DefaultAzureCredential()
        self.service_client = service_client or DataLakeServiceClient(
            account_url=config.account_url,
            credential=self.credential,
        )
        self.file_system_client = self.service_client.get_file_system_client(
            config.file_system
        )

    def file_exists(self, remote_path: str) -> tuple[bool, int | None]:
        normalized_path = normalize_remote_path(remote_path)
        file_client = self.file_system_client.get_file_client(normalized_path)
        try:
            properties = file_client.get_file_properties()
            return True, _property_size(properties)
        except ResourceNotFoundError:
            return False, None
        except AzureError as exc:
            raise self._operation_error("check", normalized_path, exc) from exc

    def upload_file(
        self, local_path: Path, remote_path: str, *, overwrite: bool = False
    ) -> UploadResult:
        normalized_path = normalize_remote_path(remote_path)
        if not local_path.is_file():
            raise FileNotFoundError(f"Local upload source does not exist: {local_path}")

        local_size = local_path.stat().st_size
        exists, remote_size = self.file_exists(normalized_path)
        if exists and not overwrite:
            if remote_size != local_size:
                raise RemoteFileConflictError(
                    f"Remote file exists with size {remote_size}, local size is {local_size}; "
                    "use --overwrite only after reviewing the conflict"
                )
            logger.info("Skipping existing ADLS file: %s (%s bytes)", normalized_path, remote_size)
            return UploadResult(
                local_path, normalized_path, "skipped", local_size, remote_size, 0
            )

        self._ensure_parent_directories(normalized_path)
        file_client = self.file_system_client.get_file_client(normalized_path)
        staging_path = f"{normalized_path}.upload-{uuid4().hex}.tmp"
        staging_client = self.file_system_client.get_file_client(staging_path)
        logger.info(
            "Uploading %s to %s/%s (%s bytes)",
            local_path,
            self.config.file_system,
            normalized_path,
            local_size,
        )
        try:
            staging_client.create_file()
            offset = 0
            with local_path.open("rb") as source_file:
                while chunk := source_file.read(UPLOAD_CHUNK_SIZE):
                    staging_client.append_data(
                        chunk, offset=offset, length=len(chunk)
                    )
                    offset += len(chunk)
            staging_client.flush_data(offset)
            staged_size = _property_size(staging_client.get_file_properties())
            if staged_size != local_size:
                raise AzureStorageOperationError(
                    f"Staged size verification failed for {normalized_path}: "
                    f"expected {local_size}, received {staged_size}"
                )
            if exists and overwrite:
                file_client.delete_file()
            published_client = staging_client.rename_file(
                f"{self.config.file_system}/{normalized_path}"
            )
            properties = published_client.get_file_properties()
        except AzureError as exc:
            self._remove_staging_file(staging_client, staging_path)
            raise self._operation_error("upload", normalized_path, exc) from exc
        except Exception:
            self._remove_staging_file(staging_client, staging_path)
            raise

        uploaded_size = _property_size(properties)
        if uploaded_size != local_size:
            raise AzureStorageOperationError(
                f"Remote size verification failed for {normalized_path}: "
                f"expected {local_size}, received {uploaded_size}"
            )
        logger.info("Uploaded and verified %s bytes at %s", uploaded_size, normalized_path)
        return UploadResult(
            local_path, normalized_path, "uploaded", local_size, uploaded_size, local_size
        )

    def _remove_staging_file(self, staging_client: Any, staging_path: str) -> None:
        try:
            staging_client.delete_file()
        except ResourceNotFoundError:
            return
        except AzureError as cleanup_error:
            error_code = (
                getattr(cleanup_error, "error_code", None)
                or type(cleanup_error).__name__
            )
            logger.warning(
                "Could not remove failed staging file %s (%s)", staging_path, error_code
            )

    def upload_directory(
        self, local_directory: Path, remote_prefix: str, *, overwrite: bool = False
    ) -> list[UploadResult]:
        if not local_directory.is_dir():
            raise NotADirectoryError(f"Local upload directory does not exist: {local_directory}")
        normalized_prefix = normalize_remote_path(remote_prefix).rstrip("/")
        results: list[UploadResult] = []
        for local_file in sorted(path for path in local_directory.rglob("*") if path.is_file()):
            relative_path = local_file.relative_to(local_directory)
            destination = PurePosixPath(normalized_prefix, *relative_path.parts).as_posix()
            results.append(self.upload_file(local_file, destination, overwrite=overwrite))
        return results

    def _ensure_parent_directories(self, remote_path: str) -> None:
        parent = PurePosixPath(remote_path).parent
        if str(parent) == ".":
            return
        current = PurePosixPath()
        for part in parent.parts:
            current = current / part
            directory_client = self.file_system_client.get_directory_client(
                current.as_posix()
            )
            try:
                directory_client.create_directory()
            except ResourceExistsError:
                continue
            except AzureError as exc:
                raise self._operation_error("create directory", current.as_posix(), exc) from exc

    @staticmethod
    def _operation_error(operation: str, remote_path: str, exc: AzureError) -> AzureStorageOperationError:
        error_code = getattr(exc, "error_code", None) or type(exc).__name__
        return AzureStorageOperationError(
            f"ADLS {operation} failed for {remote_path} ({error_code})"
        )
