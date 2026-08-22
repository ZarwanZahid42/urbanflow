"""CLI for uploading existing local UrbanFlow Bronze files to ADLS Gen2."""

from __future__ import annotations

import argparse
import logging
import sys
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

from src.ingestion.config import IngestionConfig

from .audit import AzureUploadAudit, AzureUploadAuditRecord
from .client import ADLSClient, UploadResult, remote_path_for
from .config import AzureStorageConfig

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class UploadTarget:
    source: str
    local_path: Path
    remote_path: str
    optional: bool = False


@dataclass(frozen=True)
class UploadRunSummary:
    results: list[UploadResult]
    failures: list[str]
    optional_skips: int


def utc_now() -> str:
    return datetime.now(UTC).isoformat()


def build_upload_targets(
    source: str,
    azure_config: AzureStorageConfig,
    ingestion_config: IngestionConfig,
) -> list[UploadTarget]:
    """Select only the configured development month and available source categories."""

    data_dir = azure_config.data_dir
    targets: list[UploadTarget] = []
    if source in {"tlc", "all"}:
        local_path = (
            data_dir
            / "bronze"
            / "tlc"
            / ingestion_config.tlc_taxi_type
            / f"year={ingestion_config.tlc_year:04d}"
            / f"month={ingestion_config.tlc_month:02d}"
            / "source.parquet"
        )
        targets.append(UploadTarget("tlc", local_path, remote_path_for(local_path, data_dir)))

    if source in {"taxi-zones", "all"}:
        local_path = (
            data_dir / "bronze" / "reference" / "taxi_zones" / "taxi_zone_lookup.csv"
        )
        targets.append(
            UploadTarget("taxi-zones", local_path, remote_path_for(local_path, data_dir))
        )

    if source in {"weather", "all"}:
        local_path = (
            data_dir
            / "bronze"
            / "weather"
            / f"year={ingestion_config.noaa_start_date.year:04d}"
            / f"month={ingestion_config.noaa_start_date.month:02d}"
            / "observations.json"
        )
        targets.append(
            UploadTarget("weather", local_path, remote_path_for(local_path, data_dir), optional=True)
        )
    return targets


def _audit_record(
    *,
    target: UploadTarget,
    config: AzureStorageConfig,
    run_id: str,
    started_at: str,
    status: str,
    bytes_uploaded: int,
    error_message: str | None = None,
) -> AzureUploadAuditRecord:
    return AzureUploadAuditRecord(
        run_id=run_id,
        source=target.source,
        local_path=str(target.local_path.resolve()),
        remote_path=target.remote_path,
        storage_account=config.account_name,
        filesystem=config.file_system,
        started_at=started_at,
        completed_at=utc_now(),
        status=status,
        bytes_uploaded=bytes_uploaded,
        error_message=error_message,
    )


def run_upload(
    source: str,
    azure_config: AzureStorageConfig,
    ingestion_config: IngestionConfig,
    *,
    overwrite: bool = False,
    client: ADLSClient | None = None,
) -> UploadRunSummary:
    audit = AzureUploadAudit(azure_config.audit_path)
    targets = build_upload_targets(source, azure_config, ingestion_config)
    storage_client = client or ADLSClient(azure_config)
    results: list[UploadResult] = []
    failures: list[str] = []
    optional_skips = 0

    for target in targets:
        run_id = str(uuid4())
        started_at = utc_now()
        if not target.local_path.is_file() and target.optional:
            message = "Optional local weather file is not available; upload skipped."
            logger.warning("%s Expected path: %s", message, target.local_path)
            audit.append(
                _audit_record(
                    target=target,
                    config=azure_config,
                    run_id=run_id,
                    started_at=started_at,
                    status="skipped",
                    bytes_uploaded=0,
                    error_message=message,
                )
            )
            optional_skips += 1
            continue

        try:
            result = storage_client.upload_file(
                target.local_path, target.remote_path, overwrite=overwrite
            )
            results.append(result)
            audit.append(
                _audit_record(
                    target=target,
                    config=azure_config,
                    run_id=run_id,
                    started_at=started_at,
                    status=result.status,
                    bytes_uploaded=result.bytes_uploaded,
                )
            )
        except Exception as exc:
            message = str(exc)
            failures.append(f"{target.source}: {message}")
            audit.append(
                _audit_record(
                    target=target,
                    config=azure_config,
                    run_id=run_id,
                    started_at=started_at,
                    status="failed",
                    bytes_uploaded=0,
                    error_message=message,
                )
            )
            logger.error("Upload failed for %s: %s", target.remote_path, message)

    return UploadRunSummary(results, failures, optional_skips)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Upload existing UrbanFlow Bronze files to the configured ADLS Gen2 filesystem"
    )
    parser.add_argument(
        "--source",
        choices=("tlc", "taxi-zones", "weather", "all"),
        default="all",
        help="Local source category to upload (default: all available configured sources)",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Explicitly replace remote files after local review",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    logging.getLogger("azure").setLevel(logging.WARNING)
    try:
        azure_config = AzureStorageConfig.from_env()
        ingestion_config = IngestionConfig.from_env()
        summary = run_upload(
            args.source,
            azure_config,
            ingestion_config,
            overwrite=args.overwrite,
        )
    except Exception:
        logger.exception("Azure upload command failed before source processing completed")
        return 1

    for result in summary.results:
        logger.info(
            "%s: %s -> %s (%s bytes uploaded)",
            result.status,
            result.local_path,
            result.remote_path,
            result.bytes_uploaded,
        )
    if summary.optional_skips:
        logger.info("Optional sources skipped: %s", summary.optional_skips)
    if summary.failures:
        logger.error("Upload failures: %s", len(summary.failures))
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
