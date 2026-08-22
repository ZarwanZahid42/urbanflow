import json
from datetime import date
from pathlib import Path
from unittest.mock import Mock

from azure_storage.audit import AzureUploadAudit, AzureUploadAuditRecord
from azure_storage.client import UploadResult
from azure_storage.config import AzureStorageConfig
from azure_storage.uploader import build_upload_targets, run_upload
from ingestion.config import IngestionConfig


def make_configs(tmp_path: Path) -> tuple[AzureStorageConfig, IngestionConfig]:
    azure_config = AzureStorageConfig("urbanflowdata2026", "urbanflow", tmp_path)
    ingestion_config = IngestionConfig(
        tlc_year=2026,
        tlc_month=5,
        tlc_taxi_type="yellow",
        noaa_api_token=None,
        noaa_station_id="GHCND:USW00094728",
        noaa_dataset_id="GHCND",
        noaa_start_date=date(2026, 5, 1),
        noaa_end_date=date(2026, 5, 31),
        noaa_units="metric",
        data_dir=tmp_path,
    )
    return azure_config, ingestion_config


def test_filesystem_and_source_path_construction(tmp_path: Path) -> None:
    azure_config, ingestion_config = make_configs(tmp_path)

    targets = build_upload_targets("all", azure_config, ingestion_config)

    assert azure_config.account_url == "https://urbanflowdata2026.dfs.core.windows.net"
    assert azure_config.file_system == "urbanflow"
    assert [target.remote_path for target in targets] == [
        "bronze/tlc/yellow/year=2026/month=05/source.parquet",
        "bronze/reference/taxi_zones/taxi_zone_lookup.csv",
        "bronze/weather/year=2026/month=05/observations.json",
    ]


def test_missing_weather_is_optional_and_audited(tmp_path: Path) -> None:
    azure_config, ingestion_config = make_configs(tmp_path)
    client = Mock()

    summary = run_upload(
        "weather", azure_config, ingestion_config, client=client
    )

    assert summary.optional_skips == 1
    assert summary.failures == []
    client.upload_file.assert_not_called()
    record = json.loads(azure_config.audit_path.read_text(encoding="utf-8"))
    assert record["source"] == "weather"
    assert record["status"] == "skipped"
    assert record["bytes_uploaded"] == 0


def test_upload_result_is_audited(tmp_path: Path) -> None:
    azure_config, ingestion_config = make_configs(tmp_path)
    local_path = tmp_path / "bronze/reference/taxi_zones/taxi_zone_lookup.csv"
    local_path.parent.mkdir(parents=True)
    local_path.write_bytes(b"zones")
    client = Mock()
    client.upload_file.return_value = UploadResult(
        local_path,
        "bronze/reference/taxi_zones/taxi_zone_lookup.csv",
        "uploaded",
        5,
        5,
        5,
    )

    summary = run_upload(
        "taxi-zones", azure_config, ingestion_config, client=client
    )

    assert summary.failures == []
    record = json.loads(azure_config.audit_path.read_text(encoding="utf-8"))
    assert record["storage_account"] == "urbanflowdata2026"
    assert record["filesystem"] == "urbanflow"
    assert record["remote_path"] == "bronze/reference/taxi_zones/taxi_zone_lookup.csv"
    assert record["bytes_uploaded"] == 5


def test_upload_failure_is_audited(tmp_path: Path) -> None:
    azure_config, ingestion_config = make_configs(tmp_path)
    local_path = tmp_path / "bronze/reference/taxi_zones/taxi_zone_lookup.csv"
    local_path.parent.mkdir(parents=True)
    local_path.write_bytes(b"zones")
    client = Mock()
    client.upload_file.side_effect = RuntimeError("mock upload failure")

    summary = run_upload(
        "taxi-zones", azure_config, ingestion_config, client=client
    )

    assert summary.failures == ["taxi-zones: mock upload failure"]
    record = json.loads(azure_config.audit_path.read_text(encoding="utf-8"))
    assert record["status"] == "failed"
    assert record["error_message"] == "mock upload failure"


def test_azure_audit_record_creation(tmp_path: Path) -> None:
    path = tmp_path / "audit.jsonl"
    AzureUploadAudit(path).append(
        AzureUploadAuditRecord(
            run_id="run-azure-1",
            source="tlc",
            local_path="data/source.parquet",
            remote_path="bronze/source.parquet",
            storage_account="urbanflowdata2026",
            filesystem="urbanflow",
            started_at="2026-08-22T00:00:00+00:00",
            completed_at="2026-08-22T00:01:00+00:00",
            status="uploaded",
            bytes_uploaded=10,
        )
    )

    record = json.loads(path.read_text(encoding="utf-8"))
    assert record["run_id"] == "run-azure-1"
    assert record["error_message"] is None
