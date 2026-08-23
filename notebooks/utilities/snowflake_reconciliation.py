# Databricks notebook source
"""Snowflake reconciliation audit writer for Phase 7 notebooks."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any, Iterable, Mapping

try:
    from notebooks.utilities.snowflake_common import SnowflakeConfig, qualified_table
    from notebooks.utilities.snowflake_runtime import execute
except ModuleNotFoundError:
    # Databricks %run executes prerequisite utilities in the notebook namespace.
    pass


def append_reconciliation_results(
    connection: Any,
    config: SnowflakeConfig,
    run_id: str,
    rows: Iterable[Mapping[str, Any]],
) -> None:
    table = qualified_table(config, config.audit_schema, "RECONCILIATION_RESULTS")
    statement = (
        f"INSERT INTO {table} (RUN_ID, DATASET, CHECK_NAME, METRIC_VALUE, "
        "EXPECTED_VALUE, STATUS, MEASURED_AT) VALUES (%s, %s, %s, %s, %s, %s, %s)"
    )
    measured_at = datetime.now(UTC)
    for row in rows:
        status = row["status"]
        if status not in {"PASS", "FAILED"}:
            raise ValueError(f"Invalid reconciliation status: {status}")
        execute(
            connection,
            statement,
            (
                run_id,
                row["dataset"],
                row["check_name"],
                float(row["metric_value"]),
                float(row["expected_value"]),
                status,
                measured_at,
            ),
        )
    connection.commit()
