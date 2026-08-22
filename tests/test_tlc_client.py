from pathlib import Path
from unittest.mock import Mock

import pytest
import requests

from ingestion.tlc_client import TLCClient, build_tlc_trip_url


class FakeResponse:
    def __init__(self, chunks: list[bytes], error: Exception | None = None) -> None:
        self.chunks = chunks
        self.error = error

    def __enter__(self) -> "FakeResponse":
        return self

    def __exit__(self, *args: object) -> None:
        return None

    def raise_for_status(self) -> None:
        if self.error:
            raise self.error

    def iter_content(self, chunk_size: int) -> list[bytes]:
        return self.chunks


def test_tlc_url_construction() -> None:
    assert build_tlc_trip_url("yellow", 2026, 5) == (
        "https://d37ci6vzurychx.cloudfront.net/trip-data/"
        "yellow_tripdata_2026-05.parquet"
    )


def test_existing_file_is_skipped(tmp_path: Path) -> None:
    destination = tmp_path / "bronze/tlc/yellow/year=2026/month=05/source.parquet"
    destination.parent.mkdir(parents=True)
    destination.write_bytes(b"existing")
    session = Mock()

    result = TLCClient(session=session).download_month(
        taxi_type="yellow", year=2026, month=5, data_dir=tmp_path
    )

    assert result.status == "skipped"
    assert result.file_size == 8
    session.get.assert_not_called()


def test_http_failure_does_not_create_final_or_temp_file(tmp_path: Path) -> None:
    session = Mock()
    session.get.return_value = FakeResponse([], requests.HTTPError("404"))
    destination = tmp_path / "bronze/tlc/yellow/year=2026/month=05/source.parquet"

    with pytest.raises(requests.HTTPError):
        TLCClient(session=session).download_month(
            taxi_type="yellow", year=2026, month=5, data_dir=tmp_path
        )

    assert not destination.exists()
    assert not destination.with_name("source.parquet.part").exists()


def test_successful_download_uses_temporary_file_then_renames(tmp_path: Path) -> None:
    session = Mock()
    session.get.return_value = FakeResponse([b"abc", b"def"])
    destination = tmp_path / "bronze/tlc/yellow/year=2026/month=05/source.parquet"

    result = TLCClient(session=session).download_month(
        taxi_type="yellow", year=2026, month=5, data_dir=tmp_path
    )

    assert destination.read_bytes() == b"abcdef"
    assert not destination.with_name("source.parquet.part").exists()
    assert result.file_size == 6
    assert result.status == "downloaded"
