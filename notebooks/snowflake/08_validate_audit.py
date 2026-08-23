# Databricks notebook source
# MAGIC %run "../utilities/snowflake_common"

# COMMAND ----------
# MAGIC %run "../utilities/snowflake_runtime"

# COMMAND ----------
# MAGIC %run "../utilities/snowflake_widgets"

# COMMAND ----------

dbutils.widgets.text("run_id", "")
run_id = dbutils.widgets.get("run_id")
if not run_id:
    raise ValueError("run_id is required for Phase 7 audit validation")
credentials = credentials_from_widgets(dbutils)
connection = open_control_connection(credentials)
try:
    audit = qualified_table(credentials.config, credentials.config.audit_schema, "LOAD_AUDIT")
    result = query_one(
        connection,
        f"SELECT COUNT_IF(STATUS = 'FAILED') AS FAILURES, "
        f"COUNT_IF(STATUS = 'SUCCEEDED') AS SUCCESSES FROM {audit} WHERE RUN_ID = %s",
        (run_id,),
    )
    reconciliation = qualified_table(
        credentials.config, credentials.config.audit_schema, "RECONCILIATION_RESULTS"
    )
    reconciliation_result = query_one(
        connection,
        f"SELECT COUNT_IF(STATUS = 'FAILED') AS FAILURES, COUNT(*) AS CHECKS "
        f"FROM {reconciliation} WHERE RUN_ID = %s",
        (run_id,),
    )
    if (
        int(result["failures"])
        or int(result["successes"]) < len(TABLE_CONTRACTS)
        or int(reconciliation_result["failures"])
        or int(reconciliation_result["checks"]) < 20
    ):
        raise AssertionError(
            f"Incomplete or failed Phase 7 audit set: loads={result}, "
            f"reconciliation={reconciliation_result}"
        )
    print(
        {
            "status": "SUCCEEDED",
            "run_id": run_id,
            "loads": result,
            "reconciliation": reconciliation_result,
        }
    )
finally:
    connection.close()
