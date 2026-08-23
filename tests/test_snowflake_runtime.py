from unittest.mock import MagicMock

import pytest

from notebooks.utilities.snowflake_common import SnowflakeConfig, table_contract
from notebooks.utilities.snowflake_runtime import (
    SnowflakeCredentials,
    execute_transaction,
    load_credentials,
    prepare_landing_frame,
)


class FakeSecrets:
    def __init__(self, values):
        self.values = values

    def get(self, *, scope, key):
        return self.values[(scope, key)]


class FakeDbutils:
    def __init__(self, values):
        self.secrets = FakeSecrets(values)


def test_credentials_are_loaded_from_scope_and_private_key_repr_is_redacted():
    scope = "test-scope"
    values = {
        (scope, "snowflake_private_key"): "synthetic-test-value",
        (scope, "snowflake_account"): "xy12345.us-east-1",
        (scope, "snowflake_user"): "URBANFLOW_DATABRICKS_SVC",
        (scope, "snowflake_database"): "URBANFLOW",
        (scope, "snowflake_schema"): "ANALYTICS",
        (scope, "snowflake_warehouse"): "URBANFLOW_LOAD_WH",
        (scope, "snowflake_role"): "URBANFLOW_LOADER_ROLE",
    }
    credentials = load_credentials(FakeDbutils(values), scope=scope)
    assert credentials.config.user == "URBANFLOW_DATABRICKS_SVC"
    assert "synthetic-test-value" not in repr(credentials)
    assert "<redacted>" in repr(credentials)


def test_missing_secret_fails_with_actionable_sanitized_message():
    with pytest.raises(RuntimeError, match="MANUAL ACTION REQUIRED") as exc_info:
        load_credentials(FakeDbutils({}), scope="missing")
    assert "KeyError" not in str(exc_info.value)


def test_transaction_rolls_back_on_statement_failure():
    connection = MagicMock()
    cursor = connection.cursor.return_value
    cursor.execute.side_effect = [None, RuntimeError("synthetic failure")]
    with pytest.raises(RuntimeError, match="synthetic failure"):
        execute_transaction(connection, ("BEGIN", "DELETE", "INSERT", "COMMIT"))
    connection.rollback.assert_called_once_with()
    cursor.close.assert_called_once_with()


def test_credentials_repr_never_contains_key_value():
    credentials = SnowflakeCredentials(
        SnowflakeConfig(account="xy12345.us-east-1", user="URBANFLOW_DATABRICKS_SVC"),
        "private-value",
    )
    assert "private-value" not in repr(credentials)

class FakeExpression:
    def __init__(self, value):
        self.value = value

    def alias(self, name):
        return FakeExpression(("alias", self.value, name))


class FakeFunctions:
    @staticmethod
    def col(name):
        return FakeExpression(("column", name))

    @staticmethod
    def lit(value):
        return FakeExpression(("literal", value))

    @staticmethod
    def coalesce(*expressions):
        return FakeExpression(("coalesce", *(expression.value for expression in expressions)))


class FakeFrame:
    def __init__(self, columns):
        self.columns = columns
        self.selected = None

    def select(self, *expressions):
        self.selected = expressions
        return self


def test_landing_frame_defaults_only_null_financial_adjustment_to_false():
    contract = table_contract("fact_trips")
    frame = FakeFrame(contract.column_names)
    result = prepare_landing_frame(frame, contract, FakeFunctions)
    selected = {expression.value[-1]: expression.value[1] for expression in result.selected}
    assert selected["IS_FINANCIAL_ADJUSTMENT"] == (
        "coalesce",
        ("column", "is_financial_adjustment"),
        ("literal", False),
    )
    assert selected["TRIP_ID"] == ("column", "trip_id")

