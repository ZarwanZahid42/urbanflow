# Databricks notebook source
# MAGIC %run "../utilities/snowflake_common"

# COMMAND ----------
# MAGIC %run "../utilities/snowflake_runtime"

# COMMAND ----------
# MAGIC %run "../utilities/snowflake_widgets"

# COMMAND ----------

from pathlib import Path

dbutils.widgets.text("phase7_ddl_workspace_path", "")
ddl_workspace_path = dbutils.widgets.get("phase7_ddl_workspace_path")
if not ddl_workspace_path.startswith("/Users/"):
    raise ValueError("phase7_ddl_workspace_path must be an absolute workspace user path")

ddl_file = Path("/Workspace") / ddl_workspace_path.lstrip("/")
if not ddl_file.is_file():
    raise FileNotFoundError(f"Phase 7 DDL workspace file not found: {ddl_workspace_path}")

statements = [statement.strip() for statement in ddl_file.read_text(encoding="utf-8").split(";")]
statements = [statement for statement in statements if statement]
credentials = credentials_from_widgets(dbutils)
connection = open_control_connection(credentials)
try:
    for statement in statements:
        execute(connection, statement)
    connection.commit()
    print({"status": "SUCCEEDED", "ddl_statement_count": len(statements)})
except Exception:
    connection.rollback()
    raise
finally:
    connection.close()
