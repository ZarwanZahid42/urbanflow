# Databricks notebook source
# MAGIC %run "../utilities/bronze_common"

# COMMAND ----------
# MAGIC %run "../utilities/gold_common"

# COMMAND ----------
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
source_year = int(dbutils.widgets.get("source_year"))
source_month = int(dbutils.widgets.get("source_month"))
run_id = dbutils.widgets.get("run_id") or str(uuid4())
validate_batch(source_year, source_month)

credentials = credentials_from_widgets(dbutils)
connection = open_control_connection(credentials)
results = {}
try:
    for contract in TABLE_CONTRACTS:
        started_at = datetime.now(UTC)
        year = source_year if contract.replacement == "PARTITION" else None
        month = source_month if contract.replacement == "PARTITION" else None
        try:
            frame = spark.read.format("delta").load(gold_path(contract.gold_relative_path, ADLS_ROOT))
            if contract.replacement == "PARTITION":
                frame = frame.where(gold_replace_where(source_year, source_month))
            source_count = frame.count()
            if source_count <= 0:
                raise AssertionError(f"Empty Gold source for {contract.dataset}")
            clear_landing(connection, credentials.config, contract)
            write_landing_frame(frame, credentials, contract)
            landing = qualified_table(credentials.config, credentials.config.landing_schema, contract.landing_table)
            landing_count = table_count(connection, landing, source_year=year, source_month=month)
            if landing_count != source_count:
                raise AssertionError(
                    f"Landing count mismatch for {contract.dataset}: source={source_count}, landing={landing_count}"
                )
            append_audit(connection, credentials.config, SnowflakeAuditRecord(
                run_id, contract.dataset, year, month, source_count, landing_count, None,
                "LANDED", started_at, datetime.now(UTC), None, "NOT_RUN"
            ))
            results[contract.dataset] = {"source": source_count, "landing": landing_count}
        except Exception as exc:
            append_audit(connection, credentials.config, failed_audit(
                run_id=run_id, dataset=contract.dataset, started_at=started_at, error=exc,
                source_year=year, source_month=month
            ))
            raise
    print({"status": "SUCCEEDED", "run_id": run_id, "datasets": results})
finally:
    connection.close()
