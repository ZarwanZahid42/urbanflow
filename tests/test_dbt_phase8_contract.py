from pathlib import Path
import re

import yaml


DBT_ROOT = Path("dbt")
INTERMEDIATE = DBT_ROOT / "models" / "intermediate"
MARTS = DBT_ROOT / "models" / "marts"
TESTS = DBT_ROOT / "tests"
MODEL_DEPENDENCIES = {
    INTERMEDIATE / "int_trip_enriched.sql": {
        "stg_fact_trips",
        "stg_dim_date",
        "stg_dim_time",
        "stg_dim_location",
    },
    MARTS / "mart_trip_details.sql": {"int_trip_enriched"},
    MARTS / "mart_daily_mobility.sql": {"stg_agg_daily", "stg_dim_date"},
    MARTS / "mart_hourly_mobility.sql": {"stg_agg_hourly", "stg_dim_date"},
    MARTS / "mart_location_mobility.sql": {"stg_agg_location", "stg_dim_location"},
}
SINGULAR_MODEL_TESTS = {
    "assert_int_trip_enriched_row_count.sql": "int_trip_enriched",
    "assert_mart_daily_unique_key.sql": "mart_daily_mobility",
    "assert_mart_hourly_unique_key.sql": "mart_hourly_mobility",
    "assert_mart_location_unique_key.sql": "mart_location_mobility",
    "assert_mart_daily_valid_counts.sql": "mart_daily_mobility",
    "assert_mart_hourly_nonnegative_trip_count.sql": "mart_hourly_mobility",
    "assert_mart_location_nonnegative_trip_counts.sql": "mart_location_mobility",
}


def _yaml(path: Path):
    return yaml.safe_load(path.read_text(encoding="utf-8"))


def _refs(sql: str) -> set[str]:
    return set(re.findall(r"ref\('([^']+)'\)", sql))


def test_phase8_model_files_and_ref_lineage_are_exact():
    for path, expected_dependencies in MODEL_DEPENDENCIES.items():
        assert path.is_file()
        assert _refs(path.read_text(encoding="utf-8")) == expected_dependencies


def test_nonstaging_models_use_explicit_columns_and_respect_source_boundary():
    for path in MODEL_DEPENDENCIES:
        sql = path.read_text(encoding="utf-8")
        assert not re.search(r"\bselect\s+\*", sql, flags=re.IGNORECASE)
        assert "source(" not in sql.lower()
        assert "URBANFLOW.ANALYTICS" not in sql.upper()


def test_phase8_materializations_are_deliberate_and_bounded():
    project = _yaml(DBT_ROOT / "dbt_project.yml")
    models = project["models"]["urbanflow"]
    assert models["staging"]["+materialized"] == "view"
    assert models["intermediate"]["+materialized"] == "ephemeral"
    assert models["marts"]["+materialized"] == "view"
    assert "incremental" not in str(models).lower()


def test_intermediate_and_mart_models_are_documented():
    intermediate_models = {
        model["name"] for model in _yaml(INTERMEDIATE / "intermediate.yml")["models"]
    }
    mart_models = {model["name"] for model in _yaml(MARTS / "marts.yml")["models"]}
    assert intermediate_models == {"int_trip_enriched"}
    assert mart_models == {
        "mart_trip_details",
        "mart_daily_mobility",
        "mart_hourly_mobility",
        "mart_location_mobility",
    }


def test_focused_singular_model_tests_are_present_and_use_ref():
    for filename, model_name in SINGULAR_MODEL_TESTS.items():
        path = TESTS / filename
        assert path.is_file()
        sql = path.read_text(encoding="utf-8")
        assert model_name in _refs(sql)
        assert "source(" not in sql.lower()


def test_aggregate_staging_models_test_dimension_relationships():
    models = {
        model["name"]: model
        for model in _yaml(DBT_ROOT / "models" / "staging" / "staging.yml")["models"]
    }
    serialized = {
        model_name: str(model["columns"])
        for model_name, model in models.items()
    }
    assert "stg_dim_date" in serialized["stg_agg_daily"]
    assert "stg_dim_date" in serialized["stg_agg_hourly"]
    assert "stg_dim_location" in serialized["stg_agg_location"]
