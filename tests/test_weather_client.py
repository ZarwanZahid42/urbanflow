from datetime import date
from pathlib import Path
from unittest.mock import Mock

import pytest
import requests

from ingestion.weather_client import MissingNoaaTokenError, NOAAClient, NOAA_DATA_URL


def test_noaa_request_construction() -> None:
    params = NOAAClient("token").build_request_params(
        station_id="GHCND:USW00094728",
        start_date=date(2026, 5, 1),
        end_date=date(2026, 5, 31),
    )
    assert params["datasetid"] == "GHCND"
    assert params["stationid"] == "GHCND:USW00094728"
    assert params["startdate"] == "2026-05-01"
    assert params["enddate"] == "2026-05-31"
    assert params["limit"] == 1000
    assert params["offset"] == 1


def test_noaa_missing_token_behavior(tmp_path: Path) -> None:
    with pytest.raises(MissingNoaaTokenError, match="NOAA_API_TOKEN"):
        NOAAClient(None).fetch_observations(
            station_id="GHCND:USW00094728",
            start_date=date(2026, 5, 1),
            end_date=date(2026, 5, 31),
            data_dir=tmp_path,
        )


def test_noaa_http_failure_does_not_leave_file(tmp_path: Path) -> None:
    response = Mock()
    response.raise_for_status.side_effect = requests.HTTPError("server error")
    session = Mock()
    session.get.return_value = response

    with pytest.raises(requests.HTTPError):
        NOAAClient("token", session=session).fetch_observations(
            station_id="GHCND:USW00094728",
            start_date=date(2026, 5, 1),
            end_date=date(2026, 5, 31),
            data_dir=tmp_path,
        )

    destination = tmp_path / "bronze/weather/year=2026/month=05/observations.json"
    assert not destination.exists()
    assert not destination.with_name("observations.json.part").exists()
    call = session.get.call_args
    assert call.args[0] == NOAA_DATA_URL
    assert call.kwargs["headers"] == {"token": "token"}
    assert call.kwargs["params"]["stationid"] == "GHCND:USW00094728"
    assert call.kwargs["timeout"] == (10, 60)


def test_noaa_pagination_combines_results(tmp_path: Path) -> None:
    first = Mock()
    first.json.return_value = {
        "metadata": {"resultset": {"count": 1001}},
        "results": [{"id": number} for number in range(1000)],
    }
    second = Mock()
    second.json.return_value = {
        "metadata": {"resultset": {"count": 1001}},
        "results": [{"id": 1000}],
    }
    session = Mock()
    session.get.side_effect = [first, second]

    result = NOAAClient("token", session=session).fetch_observations(
        station_id="GHCND:USW00094728",
        start_date=date(2026, 5, 1),
        end_date=date(2026, 5, 31),
        data_dir=tmp_path,
    )

    assert result.record_count == 1001
    assert session.get.call_count == 2
    assert session.get.call_args_list[1].kwargs["params"]["offset"] == 1001
