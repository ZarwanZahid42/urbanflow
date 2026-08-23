from pathlib import Path

from notebooks.utilities.snowflake_common import TABLE_CONTRACTS


def test_bootstrap_sql_defines_all_landing_and_analytics_tables():
    sql = Path("sql/snowflake/01_phase7_tables.sql").read_text(encoding="utf-8").upper()
    for contract in TABLE_CONTRACTS:
        assert f"ANALYTICS.{contract.analytics_table}" in sql
        assert f"LANDING.{contract.landing_table}" in sql
    assert "AUDIT.LOAD_AUDIT" in sql
    assert "AUDIT.RECONCILIATION_RESULTS" in sql


def test_bootstrap_sql_contains_no_external_stage_or_credential_material():
    sql = Path("sql/snowflake/01_phase7_tables.sql").read_text(encoding="utf-8").upper()
    forbidden = ("CREATE STAGE", "STORAGE_INTEGRATION", "SAS", "PASSWORD", "PRIVATE_KEY")
    assert not any(term in sql for term in forbidden)
