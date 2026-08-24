from pathlib import Path
import re

import yaml

from notebooks.utilities.snowflake_common import TABLE_CONTRACTS


STAGING = Path("dbt/models/staging")
SOURCE_NAME = "urbanflow_analytics"
LOGICAL_TO_CONTRACT = {
    "fact_trips": "fact_trips",
    "dim_date": "dim_date",
    "dim_time": "dim_time",
    "dim_location": "dim_location",
    "agg_daily": "agg_daily_trips",
    "agg_location": "agg_location_trips",
    "agg_hourly": "agg_hourly_trips",
}


def _yaml(path: Path):
    return yaml.safe_load(path.read_text(encoding="utf-8"))


def _contracts():
    return {contract.dataset: contract for contract in TABLE_CONTRACTS}


def test_dbt_sources_match_the_phase7_analytics_contract():
    document = _yaml(STAGING / "sources.yml")
    source = document["sources"][0]
    assert source["name"] == SOURCE_NAME
    assert source["database"] == "{{ env_var('DBT_SNOWFLAKE_DATABASE') }}"
    assert source["schema"] == "ANALYTICS"
    assert "freshness" not in source

    tables = {table["name"]: table for table in source["tables"]}
    assert set(tables) == set(LOGICAL_TO_CONTRACT)
    contracts = _contracts()
    for logical_name, contract_name in LOGICAL_TO_CONTRACT.items():
        assert tables[logical_name]["identifier"] == contracts[contract_name].analytics_table


def test_staging_models_use_only_dbt_sources_and_explicit_columns():
    contracts = _contracts()
    for logical_name, contract_name in LOGICAL_TO_CONTRACT.items():
        path = STAGING / f"stg_{logical_name}.sql"
        sql = path.read_text(encoding="utf-8")
        assert "select *" not in sql.lower()
        assert "URBANFLOW.ANALYTICS" not in sql.upper()
        assert f"source('{SOURCE_NAME}', '{logical_name}')" in sql
        for column in contracts[contract_name].columns:
            expected = rf"\b{re.escape(column.name)}\s+as\s+{re.escape(column.name)}\b"
            assert re.search(expected, sql, flags=re.IGNORECASE), (
                f"{path} does not explicitly expose {column.name}"
            )


def test_staging_schema_documents_all_models_and_relationships():
    document = _yaml(STAGING / "staging.yml")
    models = {model["name"]: model for model in document["models"]}
    assert set(models) == {f"stg_{name}" for name in LOGICAL_TO_CONTRACT}
    fact = models["stg_fact_trips"]
    fact_columns = {column["name"]: column for column in fact["columns"]}
    for key in (
        "trip_id",
        "pickup_date_key",
        "dropoff_date_key",
        "pickup_time_key",
        "dropoff_time_key",
        "pickup_location_id",
        "dropoff_location_id",
    ):
        assert "data_tests" in fact_columns[key]
    serialized = str(fact_columns)
    assert "stg_dim_date" in serialized
    assert "stg_dim_time" in serialized
    assert "stg_dim_location" in serialized


def test_composite_source_key_tests_cover_all_phase7_aggregates():
    expected_sources = {"agg_daily", "agg_location", "agg_hourly"}
    found_sources = set()
    for path in Path("dbt/tests").glob("assert_source_agg_*_unique_key.sql"):
        sql = path.read_text(encoding="utf-8")
        match = re.search(r"source\('urbanflow_analytics', '([^']+)'\)", sql)
        assert match is not None
        assert "having count(*) > 1" in sql.lower()
        found_sources.add(match.group(1))
    assert found_sources == expected_sources
