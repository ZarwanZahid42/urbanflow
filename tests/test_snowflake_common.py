from datetime import UTC, datetime, timedelta

import pytest

from notebooks.utilities.snowflake_common import (
    DEFAULT_SECRET_SCOPE,
    TABLE_CONTRACTS,
    ReconciliationResult,
    SnowflakeAuditRecord,
    SnowflakeConfig,
    SnowflakeSecretNames,
    expected_columns,
    idempotency_status,
    landing_null_defaults,
    missing_columns,
    qualified_table,
    replacement_plan,
    snowflake_spark_options,
    table_contract,
)


def config() -> SnowflakeConfig:
    return SnowflakeConfig(account="xy12345.us-east-1", user="URBANFLOW_DATABRICKS_SVC")


def test_expected_scope_and_secret_names_are_configurable_contracts():
    assert DEFAULT_SECRET_SCOPE == "urbanflow-snowflake"
    names = SnowflakeSecretNames()
    assert names.private_key == "snowflake_private_key"
    assert names.analytics_schema == "snowflake_schema"
    names.validate()
    with pytest.raises(ValueError):
        SnowflakeSecretNames(private_key="bad key").validate()


def test_configuration_validation_and_qualified_identifiers():
    value = config()
    value.validate()
    assert value.host == "xy12345.us-east-1.snowflakecomputing.com"
    assert qualified_table(value, "ANALYTICS", "FACT_TRIPS") == (
        "URBANFLOW.ANALYTICS.FACT_TRIPS"
    )
    with pytest.raises(ValueError):
        SnowflakeConfig(account="", user="URBANFLOW_DATABRICKS_SVC").validate()
    with pytest.raises(ValueError):
        qualified_table(value, "ANALYTICS; DROP DATABASE X", "FACT_TRIPS")
    with pytest.raises(ValueError, match="must be distinct"):
        SnowflakeConfig(
            account="xy12345.us-east-1",
            user="URBANFLOW_DATABRICKS_SVC",
            analytics_schema="LANDING",
        ).validate()


def test_connector_options_use_jwt_and_supported_serverless_names_only(
    synthetic_pkcs8_pem: str, synthetic_pkcs8_payload: str
):
    options = snowflake_spark_options(config(), synthetic_pkcs8_pem, schema="LANDING")
    assert options == {
        "host": "xy12345.us-east-1.snowflakecomputing.com",
        "sfaccount": "xy12345.us-east-1",
        "sfuser": "URBANFLOW_DATABRICKS_SVC",
        "sfauthenticator": "snowflake_jwt",
        "pem_private_key": synthetic_pkcs8_payload,
        "sfdatabase": "URBANFLOW",
        "sfschema": "LANDING",
        "sfwarehouse": "URBANFLOW_LOAD_WH",
        "sfrole": "URBANFLOW_LOADER_ROLE",
        "column_mapping": "name",
        "column_mismatch_behavior": "error",
        "usestagingtable": "true",
    }
    assert "password" not in options
    assert "tempdir" not in options


def test_all_gold_tables_have_exact_unique_mappings():
    assert [contract.dataset for contract in TABLE_CONTRACTS] == [
        "fact_trips",
        "dim_date",
        "dim_time",
        "dim_location",
        "agg_daily_trips",
        "agg_location_trips",
        "agg_hourly_trips",
    ]
    for contract in TABLE_CONTRACTS:
        contract.validate()
        assert contract.landing_table == contract.analytics_table
        assert not missing_columns(expected_columns(contract), contract.column_names)
    assert len(table_contract("fact_trips").columns) == 41
    with pytest.raises(KeyError):
        table_contract("invented_table")


def test_only_nullable_gold_financial_flag_gets_a_landing_default():
    assert landing_null_defaults(table_contract("fact_trips")) == {
        "is_financial_adjustment": False
    }
    for dataset in (
        "dim_date",
        "dim_time",
        "dim_location",
        "agg_daily_trips",
        "agg_location_trips",
        "agg_hourly_trips",
    ):
        assert landing_null_defaults(table_contract(dataset)) == {}


def test_partition_plan_is_atomic_and_scoped_to_requested_slice():
    plan = replacement_plan(table_contract("fact_trips"), config(), source_year=2026, source_month=5)
    assert plan.strategy == "PARTITION"
    assert plan.statements[0] == "BEGIN"
    assert plan.statements[-1] == "COMMIT"
    assert "SOURCE_YEAR = 2026 AND SOURCE_MONTH = 5" in plan.statements[1]
    assert "URBANFLOW.LANDING.FACT_TRIPS" in plan.statements[2]
    with pytest.raises(ValueError):
        replacement_plan(table_contract("fact_trips"), config())


def test_dimension_plan_is_deterministic_full_snapshot_replacement():
    plan = replacement_plan(table_contract("dim_date"), config())
    assert plan.strategy == "SNAPSHOT"
    assert plan.statements[1] == "DELETE FROM URBANFLOW.ANALYTICS.DIM_DATE WHERE 1 = 1"
    assert "SELECT DATE_KEY, CALENDAR_DATE" in plan.statements[2]
    with pytest.raises(ValueError):
        replacement_plan(table_contract("dim_date"), config(), source_year=2026, source_month=5)


def test_reconciliation_fails_closed_for_each_required_rule():
    assert ReconciliationResult("fact", 10, 10, 10).status == "PASS"
    assert ReconciliationResult("fact", 10, 9, 9).status == "FAILED"
    assert ReconciliationResult("fact", 10, 10, 10, duplicate_key_count=1).status == "FAILED"
    assert ReconciliationResult("fact", 10, 10, 10, boundary_failure_count=1).status == "FAILED"
    assert ReconciliationResult("fact", 10, 10, 10, referential_failure_count=1).status == "FAILED"
    assert ReconciliationResult("fact", 10, 10, 10, aggregate_difference=0.01).status == "FAILED"


def test_idempotency_requires_identical_dataset_sets_and_counts():
    assert idempotency_status({"fact": 10, "dim": 2}, {"fact": 10, "dim": 2}) == "PASS"
    assert idempotency_status({"fact": 10}, {"fact": 11}) == "FAILED"
    assert idempotency_status({"fact": 10}, {"fact": 10, "dim": 2}) == "FAILED"
    with pytest.raises(ValueError):
        idempotency_status({"fact": -1}, {"fact": -1})


def test_audit_payload_validation_and_failure_contract():
    started = datetime(2026, 5, 1, tzinfo=UTC)
    record = SnowflakeAuditRecord(
        "run-1", "fact_trips", 2026, 5, 10, 10, 10, "SUCCEEDED",
        started, started + timedelta(seconds=1), None, "PASS", 1, "PASS"
    )
    assert record.as_row()["schema_version"] == "phase7-v1"
    with pytest.raises(ValueError):
        SnowflakeAuditRecord(
            "run-1", "fact_trips", 2026, 5, 10, 9, None, "FAILED",
            started, started + timedelta(seconds=1), None, "FAILED"
        ).as_row()
    with pytest.raises(ValueError):
        SnowflakeAuditRecord(
            "run-1", "fact_trips", 2026, 5, 10, 10, 10, "SUCCEEDED",
            started + timedelta(seconds=1), started, None, "PASS"
        ).as_row()
