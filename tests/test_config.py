from datetime import date
from pathlib import Path

import pytest

from ingestion.config import IngestionConfig


def test_configuration_loading(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("TLC_YEAR", "2025")
    monkeypatch.setenv("TLC_MONTH", "7")
    monkeypatch.setenv("TLC_TAXI_TYPE", "YELLOW")
    monkeypatch.setenv("NOAA_API_TOKEN", "test-token")
    monkeypatch.setenv("NOAA_STATION_ID", "GHCND:TEST")
    monkeypatch.setenv("NOAA_START_DATE", "2025-07-01")
    monkeypatch.setenv("NOAA_END_DATE", "2025-07-31")
    monkeypatch.setenv("DATA_DIR", str(tmp_path))

    config = IngestionConfig.from_env(tmp_path / "missing.env")

    assert config.tlc_year == 2025
    assert config.tlc_month == 7
    assert config.tlc_taxi_type == "yellow"
    assert config.noaa_api_token == "test-token"
    assert config.noaa_station_id == "GHCND:TEST"
    assert config.noaa_start_date == date(2025, 7, 1)
    assert config.noaa_end_date == date(2025, 7, 31)
    assert config.data_dir == tmp_path


def test_invalid_month_is_rejected(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("TLC_MONTH", "13")
    with pytest.raises(ValueError, match="TLC_MONTH"):
        IngestionConfig.from_env(tmp_path / "missing.env")
