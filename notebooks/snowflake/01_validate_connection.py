# Databricks notebook source
# MAGIC %run "../utilities/snowflake_common"

# COMMAND ----------
# MAGIC %run "../utilities/snowflake_runtime"

# COMMAND ----------
# MAGIC %run "../utilities/snowflake_widgets"

# COMMAND ----------

credentials = credentials_from_widgets(dbutils)
connection = open_control_connection(credentials)
try:
    identity = query_one(
        connection,
        "SELECT CURRENT_ACCOUNT() AS account, CURRENT_USER() AS user, "
        "CURRENT_ROLE() AS role, CURRENT_WAREHOUSE() AS warehouse",
    )
    required = [
        qualified_table(credentials.config, credentials.config.landing_schema, contract.landing_table)
        for contract in TABLE_CONTRACTS
    ] + [
        qualified_table(credentials.config, credentials.config.analytics_schema, contract.analytics_table)
        for contract in TABLE_CONTRACTS
    ] + [qualified_table(credentials.config, credentials.config.audit_schema, "LOAD_AUDIT")]
    for table in required:
        query_one(connection, f"SELECT COUNT(*) AS ROW_COUNT FROM {table} WHERE 1 = 0")
    print({"status": "SUCCEEDED", "identity": identity, "validated_table_count": len(required)})
finally:
    connection.close()
