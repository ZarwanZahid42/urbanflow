"""Streaming downloads for official NYC TLC trip and reference data."""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import requests

TLC_TRIP_BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
TAXI_ZONE_LOOKUP_URL = (
    "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
)
DOWNLOAD_CHUNK_SIZE = 1024 * 1024
PROGRESS_INTERVAL_BYTES = 25 * 1024 * 1024

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class DownloadResult:
    source_url: str
    local_path: Path
    file_size: int
    status: str


def build_tlc_trip_url(taxi_type: str, year: int, month: int) -> str:
    """Build the predictable official TLC monthly Parquet URL."""

    normalized_type = taxi_type.strip().lower()
    if normalized_type not in {"yellow", "green", "fhv", "fhvhv"}:
        raise ValueError("Unsupported TLC taxi type")
    if not 1 <= month <= 12:
        raise ValueError("Month must be between 1 and 12")
    return f"{TLC_TRIP_BASE_URL}/{normalized_type}_tripdata_{year:04d}-{month:02d}.parquet"


class TLCClient:
    """Acquire public TLC files without loading them fully into memory."""

    def __init__(self, session: Any | None = None, timeout: tuple[int, int] = (10, 120)):
        self.session = session or requests.Session()
        self.timeout = timeout

    def download_month(
        self,
        *,
        taxi_type: str,
        year: int,
        month: int,
        data_dir: Path,
        force: bool = False,
    ) -> DownloadResult:
        source_url = build_tlc_trip_url(taxi_type, year, month)
        destination = (
            data_dir
            / "bronze"
            / "tlc"
            / taxi_type
            / f"year={year:04d}"
            / f"month={month:02d}"
            / "source.parquet"
        )
        return self._download_stream(source_url, destination, force=force)

    def download_taxi_zones(self, *, data_dir: Path, force: bool = False) -> DownloadResult:
        destination = (
            data_dir / "bronze" / "reference" / "taxi_zones" / "taxi_zone_lookup.csv"
        )
        return self._download_stream(TAXI_ZONE_LOOKUP_URL, destination, force=force)

    def _download_stream(
        self, source_url: str, destination: Path, *, force: bool
    ) -> DownloadResult:
        if destination.is_file() and not force:
            file_size = destination.stat().st_size
            logger.info("Skipping existing file: %s (%s bytes)", destination, file_size)
            return DownloadResult(source_url, destination, file_size, "skipped")

        destination.parent.mkdir(parents=True, exist_ok=True)
        temporary_path = destination.with_name(f"{destination.name}.part")
        temporary_path.unlink(missing_ok=True)
        bytes_written = 0
        next_progress = PROGRESS_INTERVAL_BYTES

        logger.info("Downloading %s", source_url)
        try:
            with self.session.get(
                source_url, stream=True, timeout=self.timeout
            ) as response:
                response.raise_for_status()
                with temporary_path.open("wb") as output_file:
                    for chunk in response.iter_content(chunk_size=DOWNLOAD_CHUNK_SIZE):
                        if not chunk:
                            continue
                        output_file.write(chunk)
                        bytes_written += len(chunk)
                        if bytes_written >= next_progress:
                            logger.info("Downloaded %.1f MiB", bytes_written / 1024 / 1024)
                            next_progress += PROGRESS_INTERVAL_BYTES
            if bytes_written == 0:
                raise ValueError(f"Downloaded file is empty: {source_url}")
            os.replace(temporary_path, destination)
        except Exception:
            temporary_path.unlink(missing_ok=True)
            logger.exception("Download failed: %s", source_url)
            raise

        logger.info("Saved %s bytes to %s", bytes_written, destination)
        return DownloadResult(source_url, destination, bytes_written, "downloaded")
