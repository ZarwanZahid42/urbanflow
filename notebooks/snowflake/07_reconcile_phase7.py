# Databricks notebook source
# MAGIC %run "../utilities/snowflake_common"

# COMMAND ----------
# MAGIC %run "../utilities/snowflake_runtime"

# COMMAND ----------
# MAGIC %run "../utilities/snowflake_reconciliation"

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
    raise ValueError("run_id is required for Phase 7 reconciliation audit")

credentials = credentials_from_widgets(dbutils)
connection = open_control_connection(credentials)
results = {}
reconciliation_rows = []


def record(dataset, check_name, metric_value, expected_value, passed):
    reconciliation_rows.append(
        {
            "dataset": dataset,
            "check_name": check_name,
            "metric_value": metric_value,
            "expected_value": expected_value,
            "status": "PASS" if passed else "FAILED",
        }
    )


try:
    for contract in TABLE_CONTRACTS:
        year = source_year if contract.replacement == "PARTITION" else None
        month = source_month if contract.replacement == "PARTITION" else None
        landing = qualified_table(
            credentials.config, credentials.config.landing_schema, contract.landing_table
        )
        target = qualified_table(
            credentials.config, credentials.config.analytics_schema, contract.analytics_table
        )
        landing_count = table_count(
            connection, landing, source_year=year, source_month=month
        )
        target_count = table_count(connection, target, source_year=year, source_month=month)
        duplicates = duplicate_key_count(
            connection, target, tuple(key.upper() for key in contract.key_columns)
        )
        result = ReconciliationResult(
            contract.dataset, landing_count, landing_count, target_count, duplicates
        )
        record(
            contract.dataset,
            "landing_target_row_count_and_key_uniqueness",
            target_count,
            landing_count,
            result.status == "PASS",
        )
        results[contract.dataset] = result.status

    fact = qualified_table(credentials.config, credentials.config.analytics_schema, "FACT_TRIPS")
    date_dim = qualified_table(
        credentials.config, credentials.config.analytics_schema, "DIM_DATE"
    )
    time_dim = qualified_table(
        credentials.config, credentials.config.analytics_schema, "DIM_TIME"
    )
    location_dim = qualified_table(
        credentials.config, credentials.config.analytics_schema, "DIM_LOCATION"
    )
    batch = f"F.SOURCE_YEAR = {source_year} AND F.SOURCE_MONTH = {source_month}"
    checks = {
        "pickup_date_fk": (
            f"SELECT COUNT(*) AS VALUE FROM {fact} F LEFT JOIN {date_dim} D "
            f"ON F.PICKUP_DATE_KEY=D.DATE_KEY WHERE {batch} AND D.DATE_KEY IS NULL"
        ),
        "dropoff_date_fk": (
            f"SELECT COUNT(*) AS VALUE FROM {fact} F LEFT JOIN {date_dim} D "
            f"ON F.DROPOFF_DATE_KEY=D.DATE_KEY WHERE {batch} AND D.DATE_KEY IS NULL"
        ),
        "pickup_time_fk": (
            f"SELECT COUNT(*) AS VALUE FROM {fact} F LEFT JOIN {time_dim} D "
            f"ON F.PICKUP_TIME_KEY=D.TIME_KEY WHERE {batch} AND D.TIME_KEY IS NULL"
        ),
        "dropoff_time_fk": (
            f"SELECT COUNT(*) AS VALUE FROM {fact} F LEFT JOIN {time_dim} D "
            f"ON F.DROPOFF_TIME_KEY=D.TIME_KEY WHERE {batch} AND D.TIME_KEY IS NULL"
        ),
        "pickup_location_fk": (
            f"SELECT COUNT(*) AS VALUE FROM {fact} F LEFT JOIN {location_dim} D "
            f"ON F.PICKUP_LOCATION_ID=D.LOCATION_ID WHERE {batch} AND D.LOCATION_ID IS NULL"
        ),
        "dropoff_location_fk": (
            f"SELECT COUNT(*) AS VALUE FROM {fact} F LEFT JOIN {location_dim} D "
            f"ON F.DROPOFF_LOCATION_ID=D.LOCATION_ID WHERE {batch} AND D.LOCATION_ID IS NULL"
        ),
    }
    failures = {name: int(query_one(connection, sql)["value"]) for name, sql in checks.items()}
    for name, value in failures.items():
        record("fact_trips", name, value, 0, value == 0)

    fact_count = table_count(
        connection, fact, source_year=source_year, source_month=source_month
    )
    daily = qualified_table(
        credentials.config, credentials.config.analytics_schema, "AGG_DAILY_TRIPS"
    )
    hourly = qualified_table(
        credentials.config, credentials.config.analytics_schema, "AGG_HOURLY_TRIPS"
    )
    location = qualified_table(
        credentials.config, credentials.config.analytics_schema, "AGG_LOCATION_TRIPS"
    )
    predicate = f"SOURCE_YEAR = {source_year} AND SOURCE_MONTH = {source_month}"
    totals = {
        "daily_trip_count": (daily, "TRIP_COUNT"),
        "hourly_trip_count": (hourly, "TRIP_COUNT"),
        "location_pickup_count": (location, "PICKUP_TRIP_COUNT"),
        "location_dropoff_count": (location, "DROPOFF_TRIP_COUNT"),
    }
    total_values = {}
    for name, (table, measure) in totals.items():
        value = int(
            query_one(
                connection,
                f"SELECT COALESCE(SUM({measure}),0) AS VALUE FROM {table} WHERE {predicate}",
            )["value"]
        )
        total_values[name] = value
        record(name.split("_", 1)[0], name, value, fact_count, value == fact_count)

    fact_revenue = float(
        query_one(
            connection,
            f"SELECT COALESCE(SUM(TOTAL_AMOUNT),0) AS VALUE FROM {fact} F WHERE {batch}",
        )["value"]
    )
    revenue_tables = {
        "daily_total_revenue": daily,
        "hourly_total_revenue": hourly,
        "location_total_revenue": location,
    }
    revenue_values = {}
    for name, table in revenue_tables.items():
        value = float(
            query_one(
                connection,
                f"SELECT COALESCE(SUM(TOTAL_REVENUE),0) AS VALUE FROM {table} "
                f"WHERE {predicate}",
            )["value"]
        )
        revenue_values[name] = value
        record(name.split("_", 1)[0], name, value, fact_revenue, abs(value - fact_revenue) < 0.005)

    append_reconciliation_results(
        connection, credentials.config, run_id, reconciliation_rows
    )
    failed_checks = [row for row in reconciliation_rows if row["status"] == "FAILED"]
    if failed_checks:
        raise AssertionError(f"Snowflake reconciliation failed: {failed_checks}")
    print(
        {
            "status": "SUCCEEDED",
            "run_id": run_id,
            "datasets": results,
            "foreign_keys": failures,
            "aggregate_totals": total_values,
            "aggregate_revenues": revenue_values,
        }
    )
finally:
    connection.close()
