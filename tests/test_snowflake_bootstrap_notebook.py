from pathlib import Path


def test_bootstrap_notebook_uses_reviewed_workspace_ddl_and_secret_contract():
    source = Path("notebooks/snowflake/00_apply_table_ddl.py").read_text(encoding="utf-8")
    assert 'dbutils.widgets.text("phase7_ddl_workspace_path", "")' in source
    assert 'Path("/Workspace")' in source
    assert "credentials_from_widgets(dbutils)" in source
    assert "for statement in statements:" in source
    assert "connection.commit()" in source
    assert "connection.rollback()" in source
    assert "PRIVATE KEY" not in source
