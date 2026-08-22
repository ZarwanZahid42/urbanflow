"""NOAA/NCEI Climate Data Online weather acquisition client."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any

import requests

NOAA_DATA_URL = "https://www.ncei.noaa.gov/cdo-web/api/v2/data"
NOAA_PAGE_SIZE = 1000


class MissingNoaaTokenError(ValueError):
    """Raised when live NOAA acquisition is requested without a token."""


@dataclass(frozen=True)
class WeatherResult:
    source_url: str
    local_path: Path
    record_count: int
    file_size: int
    status: str


class NOAAClient:
    """Paginated client for NOAA Climate Data Online observations."""

    def __init__(
        self,
        token: str | None,
        session: Any | None = None,
        timeout: tuple[int, int] = (10, 60),
    ) -> None:
        self.token = token.strip() if token else None
        self.session = session or requests.Session()
        self.timeout = timeout

    def build_request_params(
        self,
        *,
        station_id: str,
        start_date: date,
        end_date: date,
        dataset_id: str = "GHCND",
        units: str = "metric",
        offset: int = 1,
    ) -> dict[str, str | int]:
        if start_date > end_date:
            raise ValueError("start_date must be on or before end_date")
        return {
            "datasetid": dataset_id,
            "stationid": station_id,
            "startdate": start_date.isoformat(),
            "enddate": end_date.isoformat(),
            "units": units,
            "limit": NOAA_PAGE_SIZE,
            "offset": offset,
            "includemetadata": "true",
        }

    def fetch_observations(
        self,
        *,
        station_id: str,
        start_date: date,
        end_date: date,
        data_dir: Path,
        dataset_id: str = "GHCND",
        units: str = "metric",
        force: bool = False,
    ) -> WeatherResult:
        if not self.token:
            raise MissingNoaaTokenError(
                "NOAA_API_TOKEN is not configured; live weather ingestion was skipped."
            )

        destination = (
            data_dir
            / "bronze"
            / "weather"
            / f"year={start_date.year:04d}"
            / f"month={start_date.month:02d}"
            / "observations.json"
        )
        if destination.is_file() and not force:
            payload = json.loads(destination.read_text(encoding="utf-8"))
            return WeatherResult(
                NOAA_DATA_URL,
                destination,
                len(payload.get("results", [])),
                destination.stat().st_size,
                "skipped",
            )

        destination.parent.mkdir(parents=True, exist_ok=True)
        temporary_path = destination.with_name(f"{destination.name}.part")
        temporary_path.unlink(missing_ok=True)
        all_results: list[dict[str, Any]] = []
        offset = 1

        try:
            while True:
                params = self.build_request_params(
                    station_id=station_id,
                    start_date=start_date,
                    end_date=end_date,
                    dataset_id=dataset_id,
                    units=units,
                    offset=offset,
                )
                response = self.session.get(
                    NOAA_DATA_URL,
                    headers={"token": self.token},
                    params=params,
                    timeout=self.timeout,
                )
                response.raise_for_status()
                page = response.json()
                results = page.get("results", [])
                all_results.extend(results)
                result_set = page.get("metadata", {}).get("resultset", {})
                total_count = int(result_set.get("count", len(all_results)))
                if not results or len(all_results) >= total_count or len(results) < NOAA_PAGE_SIZE:
                    break
                offset += len(results)

            payload = {
                "source": "NOAA/NCEI Climate Data Online API v2",
                "source_url": NOAA_DATA_URL,
                "request": {
                    "dataset_id": dataset_id,
                    "station_id": station_id,
                    "start_date": start_date.isoformat(),
                    "end_date": end_date.isoformat(),
                    "units": units,
                },
                "record_count": len(all_results),
                "results": all_results,
            }
            temporary_path.write_text(
                json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8"
            )
            os.replace(temporary_path, destination)
        except Exception:
            temporary_path.unlink(missing_ok=True)
            raise

        return WeatherResult(
            NOAA_DATA_URL,
            destination,
            len(all_results),
            destination.stat().st_size,
            "downloaded",
        )
