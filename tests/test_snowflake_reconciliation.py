from pathlib import Path
from unittest.mock import MagicMock

import pytest

from notebooks.utilities.snowflake_common import TABLE_CONTRACTS, SnowflakeConfig
from notebooks.utilities.snowflake_reconciliation import append_reconciliation_results


def test_reconciliation_results_are_parameterized_and_committed():
    connection = MagicMock()
    cursor = connection.cursor.return_value
    config = SnowflakeConfig(account="xy12345.us-east-1", user="URBANFLOW_DATABRICKS_SVC")
    append_reconciliation_results(
        connection,
        config,
        "run-1",
        [
            {
                "dataset": "fact_trips",
                "check_name": "row_count",
                "metric_value": 10,
                "expected_value": 10,
                "status": "PASS",
            }
        ],
    )
    statement, parameters = cursor.execute.call_args.args
    assert "URBANFLOW.AUDIT.RECONCILIATION_RESULTS" in statement
    assert "%s" in statement
    assert parameters[:5] == ("run-1", "fact_trips", "row_count", 10.0, 10.0)
    connection.commit.assert_called_once_with()


def test_reconciliation_writer_rejects_unknown_status():
    connection = MagicMock()
    config = SnowflakeConfig(account="xy12345.us-east-1", user="URBANFLOW_DATABRICKS_SVC")
    with pytest.raises(ValueError, match="Invalid reconciliation status"):
        append_reconciliation_results(
            connection,
            config,
            "run-1",
            [
                {
                    "dataset": "fact_trips",
                    "check_name": "row_count",
                    "metric_value": 9,
                    "expected_value": 10,
                    "status": "WARNING",
                }
            ],
        )


def test_snowflake_ddl_matches_every_contract_column_and_type():
    sql = Path("sql/snowflake/01_phase7_tables.sql").read_text(encoding="utf-8").upper()
    for contract in TABLE_CONTRACTS:
        start = sql.index(f"CREATE TABLE IF NOT EXISTS ANALYTICS.{contract.analytics_table} (")
        end = sql.index(");", start)
        definition = sql[start:end]
        for column in contract.columns:
            assert f"{column.name.upper()} {column.snowflake_type}" in definition


def test_ordered_serverless_workflow_and_reconciliation_coverage_are_present():
    folder = Path("notebooks/snowflake")
    notebooks = sorted(path.name for path in folder.glob("[0-9][0-9]_*.py"))
    assert notebooks == [
        "00_apply_table_ddl.py",
        "01_validate_connection.py",
        "02_load_landing.py",
        "03_validate_landing.py",
        "04_load_fact_trips.py",
        "05_load_dimensions.py",
        "06_load_aggregates.py",
        "07_reconcile_phase7.py",
        "08_validate_audit.py",
        "09_validate_idempotency.py",
    ]
    reconciliation = (folder / "07_reconcile_phase7.py").read_text(encoding="utf-8")
    for relationship in (
        "pickup_date_fk",
        "dropoff_date_fk",
        "pickup_time_fk",
        "dropoff_time_fk",
        "pickup_location_fk",
        "dropoff_location_fk",
    ):
        assert relationship in reconciliation
    assert "SUM(TOTAL_REVENUE)" in reconciliation
