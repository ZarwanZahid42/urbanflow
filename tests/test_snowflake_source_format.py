from pathlib import Path


def test_databricks_run_magics_are_quoted_and_isolated_cells():
    for notebook in Path("notebooks/snowflake").glob("[0-9][0-9]_*.py"):
        lines = notebook.read_text(encoding="utf-8").splitlines()
        magic_indexes = [
            index for index, line in enumerate(lines) if line.startswith("# MAGIC %run")
        ]
        assert magic_indexes
        for index in magic_indexes:
            assert lines[index].startswith('# MAGIC %run "../utilities/')
            assert lines[index].endswith('"')
            assert lines[index + 1] == ""
            assert lines[index + 2] == "# COMMAND ----------"
