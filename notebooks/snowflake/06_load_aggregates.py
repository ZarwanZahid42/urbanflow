# Databricks notebook source
# MAGIC %run "../utilities/snowflake_common"

# COMMAND ----------
# MAGIC %run "../utilities/snowflake_runtime"

# COMMAND ----------
# MAGIC %run "../utilities/snowflake_widgets"

# COMMAND ----------

from datetime import UTC, datetime
from uuid import uuid4

dbutils.widgets.text("source_year", "2026")
dbutils.widgets.text("source_month", "5")
dbutils.widgets.text("run_id", "")
dbutils.widgets.text("idempotency_pass", "1")
source_year = int(dbutils.widgets.get("source_year"))
source_month = int(dbutils.widgets.get("source_month"))
run_id = dbutils.widgets.get("run_id") or str(uuid4())
idempotency_pass = int(dbutils.widgets.get("idempotency_pass"))
credentials = credentials_from_widgets(dbutils)
connection = open_control_connection(credentials)
results = {}
try:
    for name in ("agg_daily_trips", "agg_location_trips", "agg_hourly_trips"):
        started_at = datetime.now(UTC)
        contract = table_contract(name)
        landing = qualified_table(credentials.config, credentials.config.landing_schema, contract.landing_table)
        target = qualified_table(credentials.config, credentials.config.analytics_schema, contract.analytics_table)
        try:
            landing_count = table_count(connection, landing, source_year=source_year, source_month=source_month)
            execute_transaction(connection, replacement_plan(
                contract, credentials.config, source_year=source_year, source_month=source_month
            ).statements)
            target_count = table_count(connection, target, source_year=source_year, source_month=source_month)
            if target_count != landing_count:
                raise AssertionError(f"Aggregate count mismatch: {name}")
            append_audit(connection, credentials.config, SnowflakeAuditRecord(
                run_id, name, source_year, source_month, landing_count, landing_count, target_count,
                "SUCCEEDED", started_at, datetime.now(UTC), None, "PASS", idempotency_pass
            ))
            results[name] = target_count
        except Exception as exc:
            append_audit(connection, credentials.config, failed_audit(
                run_id=run_id, dataset=name, started_at=started_at, error=exc,
                source_year=source_year, source_month=source_month
            ))
            raise
    print({"status": "SUCCEEDED", "aggregates": results})
finally:
    connection.close()
