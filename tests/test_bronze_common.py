from notebooks.utilities.bronze_common import (
    ADLS_ROOT,
    FILE_PATH_METADATA_COLUMN,
    SOURCE_MONTH_COLUMN,
    SOURCE_YEAR_COLUMN,
    add_ingestion_metadata,
    adls_path,
    missing_columns,
    quality_status,
    schema_version,
    yellow_raw_path,
    yellow_replace_where,
)


class FakeSparkFunctions:
    @staticmethod
    def col(name):
        return ("col", name)

    @staticmethod
    def current_timestamp():
        return ("current_timestamp",)

    @staticmethod
    def lit(value):
        return ("lit", value)


class FakeMetadataDataFrame:
    def __init__(self):
        self.columns_added = []

    def withColumn(self, name, expression):
        self.columns_added.append((name, expression))
        return self


def test_paths_are_canonical_and_batch_is_zero_padded():
    assert yellow_raw_path(2026, 5) == (
        f"{ADLS_ROOT}/bronze/tlc/yellow/year=2026/month=05/source.parquet"
    )
    assert adls_path("/bronze/delta/taxi_zones/") == f"{ADLS_ROOT}/bronze/delta/taxi_zones"


def test_adls_path_rejects_absolute_and_parent_paths():
    for path in ("", "abfss://other/path", "bronze/../silver"):
        try:
            adls_path(path)
        except ValueError:
            pass
        else:
            raise AssertionError(f"Expected invalid path: {path}")


def test_replace_predicate_is_exact_batch_boundary():
    assert yellow_replace_where(2026, 5) == (
        f"{SOURCE_YEAR_COLUMN} = 2026 AND {SOURCE_MONTH_COLUMN} = 5"
    )


def test_schema_version_is_stable_and_missing_columns_are_sorted():
    assert schema_version('{"a":1}') == schema_version('{"a":1}')
    assert schema_version('{"a":1}') != schema_version('{"a":2}')
    assert missing_columns(["a"], ["c", "a", "b"]) == ["b", "c"]


def test_quality_status_uses_zero_tolerance_warning_metrics():
    assert quality_status({"bad": 0}, ["bad"]) == "PASSED"
    assert quality_status({"bad": 1}, ["bad"]) == "WARNING"


def test_ingestion_metadata_uses_unity_catalog_file_path_column():
    dataframe = FakeMetadataDataFrame()

    add_ingestion_metadata(
        dataframe,
        run_id="run-1",
        year=2026,
        month=5,
        spark_functions=FakeSparkFunctions,
    )

    assert dataframe.columns_added[0] == (
        "_urbanflow_source_file",
        ("col", FILE_PATH_METADATA_COLUMN),
    )
    assert FILE_PATH_METADATA_COLUMN == "_metadata.file_path"
