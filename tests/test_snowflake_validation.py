from unittest.mock import MagicMock

import pytest

from notebooks.utilities.snowflake_common import table_contract
from notebooks.utilities.snowflake_validation import validate_landing_schema


def connection_with_description(rows):
    connection = MagicMock()
    connection.cursor.return_value.fetchall.return_value = rows
    return connection


def test_landing_schema_accepts_exact_names_and_snowflake_type_families():
    contract = table_contract("dim_time")
    rows = [(column.name.upper(), column.snowflake_type) for column in contract.columns]
    connection = connection_with_description(rows)
    validate_landing_schema(connection, "URBANFLOW.LANDING.DIM_TIME", contract)
    connection.cursor.return_value.close.assert_called_once_with()


def test_landing_schema_rejects_missing_or_reordered_columns():
    contract = table_contract("dim_time")
    rows = [(column.name.upper(), column.snowflake_type) for column in contract.columns][:-1]
    with pytest.raises(AssertionError, match="schema columns differ"):
        validate_landing_schema(
            connection_with_description(rows), "URBANFLOW.LANDING.DIM_TIME", contract
        )


def test_landing_schema_rejects_type_family_drift():
    contract = table_contract("dim_time")
    rows = [(column.name.upper(), column.snowflake_type) for column in contract.columns]
    rows[0] = (rows[0][0], "VARCHAR")
    with pytest.raises(AssertionError, match="Landing type differs"):
        validate_landing_schema(
            connection_with_description(rows), "URBANFLOW.LANDING.DIM_TIME", contract
        )
