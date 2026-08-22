"""Command-line entry point for UrbanFlow local source acquisition."""

from __future__ import annotations

import argparse
import logging
import sys
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

from .config import IngestionConfig
from .ingestion_audit import AuditRecord, IngestionAudit
from .tlc_client import TAXI_ZONE_LOOKUP_URL, TLCClient, build_tlc_trip_url
from .weather_client import NOAA_DATA_URL, NOAAClient

logger = logging.getLogger(__name__)


def utc_now() -> str:
    return datetime.now(UTC).isoformat()


def _run_and_audit(
    *,
    audit: IngestionAudit,
    source: str,
    dataset: str,
    source_url: str,
    operation: Callable[[], Any],
) -> Any:
    run_id = str(uuid4())
    started_at = utc_now()
    try:
        result = operation()
        count = getattr(result, "record_count", getattr(result, "file_size", 0))
        audit.append(
            AuditRecord(
                run_id=run_id,
                source=source,
                dataset=dataset,
                source_url=source_url,
                started_at=started_at,
                completed_at=utc_now(),
                status=result.status,
                records_or_bytes=count,
                local_path=str(result.local_path.resolve()),
            )
        )
        return result
    except Exception as exc:
        audit.append(
            AuditRecord(
                run_id=run_id,
                source=source,
                dataset=dataset,
                source_url=source_url,
                started_at=started_at,
                completed_at=utc_now(),
                status="failed",
                records_or_bytes=0,
                local_path="",
                error_message=str(exc),
            )
        )
        raise


def _record_weather_skip(config: IngestionConfig, audit: IngestionAudit) -> None:
    now = utc_now()
    message = "NOAA_API_TOKEN is not configured; live weather ingestion was skipped."
    audit.append(
        AuditRecord(
            run_id=str(uuid4()),
            source="noaa",
            dataset=config.noaa_dataset_id,
            source_url=NOAA_DATA_URL,
            started_at=now,
            completed_at=now,
            status="skipped",
            records_or_bytes=0,
            local_path="",
            error_message=message,
        )
    )
    logger.warning(message)


def run_source(source: str, config: IngestionConfig, *, force: bool = False) -> list[Any]:
    audit = IngestionAudit(config.data_dir / "audit" / "ingestion_audit.jsonl")
    tlc_client = TLCClient()
    results: list[Any] = []

    if source in {"tlc", "all"}:
        url = build_tlc_trip_url(
            config.tlc_taxi_type, config.tlc_year, config.tlc_month
        )
        results.append(
            _run_and_audit(
                audit=audit,
                source="nyc_tlc",
                dataset=(
                    f"{config.tlc_taxi_type}_tripdata_"
                    f"{config.tlc_year:04d}-{config.tlc_month:02d}"
                ),
                source_url=url,
                operation=lambda: tlc_client.download_month(
                    taxi_type=config.tlc_taxi_type,
                    year=config.tlc_year,
                    month=config.tlc_month,
                    data_dir=config.data_dir,
                    force=force,
                ),
            )
        )

    if source in {"taxi-zones", "all"}:
        results.append(
            _run_and_audit(
                audit=audit,
                source="nyc_tlc",
                dataset="taxi_zone_lookup",
                source_url=TAXI_ZONE_LOOKUP_URL,
                operation=lambda: tlc_client.download_taxi_zones(
                    data_dir=config.data_dir, force=force
                ),
            )
        )

    if source in {"weather", "all"}:
        if not config.noaa_api_token:
            _record_weather_skip(config, audit)
        else:
            weather_client = NOAAClient(config.noaa_api_token)
            results.append(
                _run_and_audit(
                    audit=audit,
                    source="noaa",
                    dataset=config.noaa_dataset_id,
                    source_url=NOAA_DATA_URL,
                    operation=lambda: weather_client.fetch_observations(
                        station_id=config.noaa_station_id,
                        start_date=config.noaa_start_date,
                        end_date=config.noaa_end_date,
                        data_dir=config.data_dir,
                        dataset_id=config.noaa_dataset_id,
                        units=config.noaa_units,
                        force=force,
                    ),
                )
            )
    return results


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Acquire UrbanFlow source data locally")
    parser.add_argument(
        "--source", choices=("tlc", "taxi-zones", "weather", "all"), required=True
    )
    parser.add_argument("--force", action="store_true", help="Replace existing raw files")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    try:
        config = IngestionConfig.from_env()
        results = run_source(args.source, config, force=args.force)
    except Exception:
        logger.exception("Ingestion failed")
        return 1

    for result in results:
        logger.info(
            "%s: %s (%s bytes)", result.status, Path(result.local_path), result.file_size
        )
    return 0


if __name__ == "__main__":
    sys.exit(main())
