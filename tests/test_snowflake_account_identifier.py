import pytest

from notebooks.utilities.snowflake_common import SnowflakeConfig, snowflake_spark_options


def test_organization_qualifies_a_bare_account_name_for_clients_and_host(
    synthetic_pkcs8_pem: str,
):
    config = SnowflakeConfig(
        account="TR14287",
        organization="YFSZUFO",
        user="URBANFLOW_DATABRICKS_SVC",
    )
    config.validate()
    assert config.account_identifier == "yfszufo-tr14287"
    assert config.host == "yfszufo-tr14287.snowflakecomputing.com"
    options = snowflake_spark_options(config, synthetic_pkcs8_pem, schema="LANDING")
    assert options["sfaccount"] == "yfszufo-tr14287"
    assert options["host"] == "yfszufo-tr14287.snowflakecomputing.com"


def test_already_qualified_account_identifier_is_not_prefixed_twice():
    config = SnowflakeConfig(
        account="YFSZUFO-TR14287",
        organization="YFSZUFO",
        user="URBANFLOW_DATABRICKS_SVC",
    )
    assert config.account_identifier == "yfszufo-tr14287"


def test_invalid_organization_is_rejected():
    with pytest.raises(ValueError, match="organization"):
        SnowflakeConfig(
            account="TR14287",
            organization="bad organization",
            user="URBANFLOW_DATABRICKS_SVC",
        ).validate()
