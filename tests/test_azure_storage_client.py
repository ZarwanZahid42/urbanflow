from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

import pytest
from azure.core.exceptions import AzureError, ResourceNotFoundError

from azure_storage.client import (
    ADLSClient,
    AzureStorageOperationError,
    RemoteFileConflictError,
    remote_path_for,
)
from azure_storage.config import AzureStorageConfig


def make_config(tmp_path: Path) -> AzureStorageConfig:
    return AzureStorageConfig("urbanflowdata2026", "urbanflow", tmp_path)


def make_client(tmp_path: Path, file_system_client: Mock) -> ADLSClient:
    service_client = Mock()
    service_client.get_file_system_client.return_value = file_system_client
    return ADLSClient(make_config(tmp_path), credential=Mock(), service_client=service_client)


def test_default_credential_and_client_initialization(tmp_path: Path) -> None:
    with (
        patch("azure_storage.client.DefaultAzureCredential") as credential_class,
        patch("azure_storage.client.DataLakeServiceClient") as service_class,
    ):
        service = service_class.return_value
        ADLSClient(make_config(tmp_path))

    credential_class.assert_called_once_with()
    service_class.assert_called_once_with(
        account_url="https://urbanflowdata2026.dfs.core.windows.net",
        credential=credential_class.return_value,
    )
    service.get_file_system_client.assert_called_once_with("urbanflow")


def test_remote_path_construction(tmp_path: Path) -> None:
    local_path = tmp_path / "bronze/tlc/yellow/year=2026/month=05/source.parquet"
    assert remote_path_for(local_path, tmp_path) == (
        "bronze/tlc/yellow/year=2026/month=05/source.parquet"
    )


def test_remote_file_detection(tmp_path: Path) -> None:
    file_client = Mock()
    file_client.get_file_properties.return_value = SimpleNamespace(size=42)
    file_system = Mock()
    file_system.get_file_client.return_value = file_client

    exists, size = make_client(tmp_path, file_system).file_exists("bronze/file.parquet")

    assert exists is True
    assert size == 42
    file_system.get_file_client.assert_called_once_with("bronze/file.parquet")


def test_upload_creates_required_directories_and_verifies_size(tmp_path: Path) -> None:
    local_path = tmp_path / "source.parquet"
    local_path.write_bytes(b"urbanflow")
    file_client = Mock()
    file_client.get_file_properties.side_effect = [
        ResourceNotFoundError("missing"),
        SimpleNamespace(size=9),
    ]
    published_client = Mock()
    published_client.get_file_properties.return_value = SimpleNamespace(size=9)
    file_client.rename_file.return_value = published_client
    file_system = Mock()
    file_system.get_file_client.return_value = file_client

    result = make_client(tmp_path, file_system).upload_file(
        local_path, "bronze/tlc/source.parquet"
    )

    assert result.status == "uploaded"
    assert result.bytes_uploaded == 9
    file_client.create_file.assert_called_once_with()
    file_client.append_data.assert_called_once_with(b"urbanflow", offset=0, length=9)
    file_client.flush_data.assert_called_once_with(9)
    file_client.rename_file.assert_called_once_with(
        "urbanflow/bronze/tlc/source.parquet"
    )
    created_paths = [call.args[0] for call in file_system.get_directory_client.call_args_list]
    assert created_paths == ["bronze", "bronze/tlc"]


def test_existing_same_size_file_is_skipped(tmp_path: Path) -> None:
    local_path = tmp_path / "source.parquet"
    local_path.write_bytes(b"same")
    file_client = Mock()
    file_client.get_file_properties.return_value = SimpleNamespace(size=4)
    file_system = Mock()
    file_system.get_file_client.return_value = file_client

    result = make_client(tmp_path, file_system).upload_file(
        local_path, "bronze/source.parquet"
    )

    assert result.status == "skipped"
    assert result.bytes_uploaded == 0
    file_client.append_data.assert_not_called()


def test_existing_different_size_requires_explicit_overwrite(tmp_path: Path) -> None:
    local_path = tmp_path / "source.parquet"
    local_path.write_bytes(b"local")
    file_client = Mock()
    file_client.get_file_properties.return_value = SimpleNamespace(size=3)
    file_system = Mock()
    file_system.get_file_client.return_value = file_client

    with pytest.raises(RemoteFileConflictError, match="--overwrite"):
        make_client(tmp_path, file_system).upload_file(
            local_path, "bronze/source.parquet"
        )

    file_client.append_data.assert_not_called()


def test_overwrite_uploads_existing_file(tmp_path: Path) -> None:
    local_path = tmp_path / "source.parquet"
    local_path.write_bytes(b"new")
    file_client = Mock()
    file_client.get_file_properties.side_effect = [
        SimpleNamespace(size=3),
        SimpleNamespace(size=3),
    ]
    published_client = Mock()
    published_client.get_file_properties.return_value = SimpleNamespace(size=3)
    file_client.rename_file.return_value = published_client
    file_system = Mock()
    file_system.get_file_client.return_value = file_client

    result = make_client(tmp_path, file_system).upload_file(
        local_path, "bronze/source.parquet", overwrite=True
    )

    assert result.status == "uploaded"
    file_client.delete_file.assert_called_once_with()
    file_client.append_data.assert_called_once_with(b"new", offset=0, length=3)


def test_upload_failure_is_sanitized_and_raised(tmp_path: Path) -> None:
    local_path = tmp_path / "source.parquet"
    local_path.write_bytes(b"data")
    file_client = Mock()
    file_client.get_file_properties.side_effect = ResourceNotFoundError("missing")
    file_client.append_data.side_effect = AzureError("service failed")
    file_system = Mock()
    file_system.get_file_client.return_value = file_client

    with pytest.raises(AzureStorageOperationError, match="ADLS upload failed"):
        make_client(tmp_path, file_system).upload_file(
            local_path, "bronze/source.parquet"
        )
