"""Environment-backed configuration for local source acquisition."""

from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import date
from pathlib import Path

from dotenv import load_dotenv

DEFAULT_TLC_YEAR = 2026
DEFAULT_TLC_MONTH = 5
DEFAULT_NOAA_STATION_ID = "GHCND:USW00094728"


def _optional_env(name: str) -> str | None:
    value = os.getenv(name)
    return value.strip() if value and value.strip() else None


@dataclass(frozen=True)
class IngestionConfig:
    """Settings shared by the ingestion command and source clients."""

    tlc_year: int
    tlc_month: int
    tlc_taxi_type: str
    noaa_api_token: str | None
    noaa_station_id: str
    noaa_dataset_id: str
    noaa_start_date: date
    noaa_end_date: date
    noaa_units: str
    data_dir: Path

    @classmethod
    def from_env(cls, env_file: str | Path | None = None) -> IngestionConfig:
        """Load configuration from an optional dotenv file and the process environment."""

        load_dotenv(dotenv_path=env_file, override=False)
        year = int(os.getenv("TLC_YEAR", str(DEFAULT_TLC_YEAR)))
        month = int(os.getenv("TLC_MONTH", str(DEFAULT_TLC_MONTH)))
        if not 1 <= month <= 12:
            raise ValueError("TLC_MONTH must be between 1 and 12")
        taxi_type = os.getenv("TLC_TAXI_TYPE", "yellow").strip().lower()
        start_date = date.fromisoformat(
            os.getenv("NOAA_START_DATE", f"{year:04d}-{month:02d}-01")
        )
        end_date = date.fromisoformat(
            os.getenv("NOAA_END_DATE", f"{year:04d}-{month:02d}-28")
        )
        units = os.getenv("NOAA_UNITS", "metric").strip().lower()

        if not 1 <= month <= 12:
            raise ValueError("TLC_MONTH must be between 1 and 12")
        if taxi_type not in {"yellow", "green", "fhv", "fhvhv"}:
            raise ValueError("TLC_TAXI_TYPE must be yellow, green, fhv, or fhvhv")
        if start_date > end_date:
            raise ValueError("NOAA_START_DATE must be on or before NOAA_END_DATE")
        if units not in {"metric", "standard"}:
            raise ValueError("NOAA_UNITS must be metric or standard")

        return cls(
            tlc_year=year,
            tlc_month=month,
            tlc_taxi_type=taxi_type,
            noaa_api_token=_optional_env("NOAA_API_TOKEN"),
            noaa_station_id=os.getenv(
                "NOAA_STATION_ID", DEFAULT_NOAA_STATION_ID
            ).strip(),
            noaa_dataset_id=os.getenv("NOAA_DATASET_ID", "GHCND").strip(),
            noaa_start_date=start_date,
            noaa_end_date=end_date,
            noaa_units=units,
            data_dir=Path(os.getenv("DATA_DIR", "data")).expanduser(),
        )
