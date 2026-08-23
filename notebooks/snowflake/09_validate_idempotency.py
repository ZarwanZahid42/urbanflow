# Databricks notebook source
# MAGIC %run "../utilities/snowflake_common"

# COMMAND ----------
# MAGIC %run "../utilities/snowflake_runtime"

# COMMAND ----------
# MAGIC %run "../utilities/snowflake_widgets"

# COMMAND ----------

dbutils.widgets.text("source_year", "2026")
dbutils.widgets.text("source_month", "5")
dbutils.widgets.text("run_id", "")
source_year = int(dbutils.widgets.get("source_year"))
source_month = int(dbutils.widgets.get("source_month"))
run_id = dbutils.widgets.get("run_id")
if not run_id:
    raise ValueError("run_id shared by both passes is required for idempotency validation")
credentials = credentials_from_widgets(dbutils)
connection = open_control_connection(credentials)
counts = {}
try:
    audit = qualified_table(credentials.config, credentials.config.audit_schema, "LOAD_AUDIT")
    for contract in TABLE_CONTRACTS:
        target = qualified_table(credentials.config, credentials.config.analytics_schema, contract.analytics_table)
        year = source_year if contract.replacement == "PARTITION" else None
        month = source_month if contract.replacement == "PARTITION" else None
        counts[contract.dataset] = table_count(connection, target, source_year=year, source_month=month)
        duplicate_count = duplicate_key_count(connection, target, tuple(key.upper() for key in contract.key_columns))
        if duplicate_count:
            raise AssertionError(f"Idempotency duplicate keys in {contract.dataset}: {duplicate_count}")
        audit_filter = "RUN_ID = %s AND DATASET = %s AND STATUS = 'SUCCEEDED'"
        parameters = [run_id, contract.dataset]
        if contract.replacement == "PARTITION":
            audit_filter += " AND SOURCE_YEAR = %s AND SOURCE_MONTH = %s"
            parameters.extend((source_year, source_month))
        else:
            audit_filter += " AND SOURCE_YEAR IS NULL AND SOURCE_MONTH IS NULL"
        pass_counts = query_one(
            connection,
            f"SELECT COUNT(DISTINCT IDEMPOTENCY_PASS) AS PASSES, "
            f"COUNT(DISTINCT TARGET_ROW_COUNT) AS DISTINCT_COUNTS FROM {audit} "
            f"WHERE {audit_filter}",
            tuple(parameters),
        )
        if int(pass_counts["passes"]) < 2 or int(pass_counts["distinct_counts"]) != 1:
            raise AssertionError(f"Two stable successful passes not found for {contract.dataset}: {pass_counts}")
    print({"status": "SUCCEEDED", "target_counts": counts, "idempotency": "PASS"})
finally:
    connection.close()
