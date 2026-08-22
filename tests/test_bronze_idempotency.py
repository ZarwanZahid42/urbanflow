from notebooks.utilities.bronze_common import (
    SOURCE_MONTH_COLUMN,
    SOURCE_YEAR_COLUMN,
    write_reference_delta,
    write_yellow_delta,
)


class FakeWriter:
    def __init__(self):
        self.calls = []

    def format(self, value):
        self.calls.append(("format", value))
        return self

    def mode(self, value):
        self.calls.append(("mode", value))
        return self

    def option(self, key, value):
        self.calls.append(("option", key, value))
        return self

    def partitionBy(self, *columns):
        self.calls.append(("partitionBy", *columns))
        return self

    def save(self, path):
        self.calls.append(("save", path))


class FakeDataFrame:
    def __init__(self):
        self.write = FakeWriter()


def test_yellow_write_replaces_only_one_partition():
    dataframe = FakeDataFrame()
    write_yellow_delta(dataframe, "target", 2026, 5)
    assert ("mode", "overwrite") in dataframe.write.calls
    assert (
        "option",
        "replaceWhere",
        f"{SOURCE_YEAR_COLUMN} = 2026 AND {SOURCE_MONTH_COLUMN} = 5",
    ) in dataframe.write.calls
    assert ("partitionBy", SOURCE_YEAR_COLUMN, SOURCE_MONTH_COLUMN) in dataframe.write.calls


def test_reference_write_is_unpartitioned_full_snapshot():
    dataframe = FakeDataFrame()
    write_reference_delta(dataframe, "target")
    assert ("mode", "overwrite") in dataframe.write.calls
    assert not any(call[0] == "partitionBy" for call in dataframe.write.calls)
