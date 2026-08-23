# Databricks notebook source
# MAGIC %run "../utilities/snowflake_common"

# COMMAND ----------
# MAGIC %run "../utilities/snowflake_runtime"

# COMMAND ----------
# MAGIC %run "../utilities/snowflake_validation"

# COMMAND ----------
# MAGIC %run "../utilities/snowflake_widgets"

# COMMAND ----------

dbutils.widgets.text("source_year", "2026")
dbutils.widgets.text("source_month", "5")
source_year = int(dbutils.widgets.get("source_year"))
source_month = int(dbutils.widgets.get("source_month"))
validate_batch(source_year, source_month)
credentials = credentials_from_widgets(dbutils)
connection = open_control_connection(credentials)
results = {}
try:
    for contract in TABLE_CONTRACTS:
        table = qualified_table(credentials.config, credentials.config.landing_schema, contract.landing_table)
        validate_landing_schema(connection, table, contract)
        year = source_year if contract.replacement == "PARTITION" else None
        month = source_month if contract.replacement == "PARTITION" else None
        count = table_count(connection, table, source_year=year, source_month=month)
        duplicates = duplicate_key_count(connection, table, tuple(key.upper() for key in contract.key_columns))
        null_predicate = " OR ".join(f"{key.upper()} IS NULL" for key in contract.key_columns)
        null_keys = int(query_one(connection, f"SELECT COUNT(*) AS VALUE FROM {table} WHERE {null_predicate}")["value"])
        boundary_failures = 0
        if contract.replacement == "PARTITION":
            boundary_failures = int(query_one(
                connection,
                f"SELECT COUNT(*) AS VALUE FROM {table} WHERE SOURCE_YEAR <> {source_year} OR SOURCE_MONTH <> {source_month}",
            )["value"])
        if count <= 0 or duplicates or null_keys or boundary_failures:
            raise AssertionError(
                f"Landing validation failed for {contract.dataset}: count={count}, duplicates={duplicates}, "
                f"null_keys={null_keys}, boundary_failures={boundary_failures}"
            )
        results[contract.dataset] = {"rows": count, "duplicates": duplicates}
    print({"status": "SUCCEEDED", "datasets": results})
finally:
    connection.close()
