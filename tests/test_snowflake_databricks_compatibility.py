from pathlib import Path


def test_phase7_utilities_support_databricks_percent_run_namespace():
    utilities = (
        "snowflake_runtime.py",
        "snowflake_widgets.py",
        "snowflake_validation.py",
        "snowflake_reconciliation.py",
    )
    for name in utilities:
        source = (Path("notebooks/utilities") / name).read_text(encoding="utf-8")
        assert "except ModuleNotFoundError:" in source
        assert "Databricks %run" in source


def test_landing_notebook_declares_existing_gold_contract_dependencies():
    source = Path("notebooks/snowflake/02_load_landing.py").read_text(encoding="utf-8")
    assert '%run "../utilities/bronze_common"' in source
    assert '%run "../utilities/gold_common"' in source
