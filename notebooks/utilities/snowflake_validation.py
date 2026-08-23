# Databricks notebook source
"""Snowflake landing schema validation kept independent of PySpark."""

from __future__ import annotations

from typing import Any

try:
    from notebooks.utilities.snowflake_common import TableContract, expected_columns
except ModuleNotFoundError:
    # Databricks %run executes snowflake_common first in the notebook namespace.
    pass


def validate_landing_schema(
    connection: Any, qualified_landing_table: str, contract: TableContract
) -> None:
    cursor = connection.cursor()
    try:
        cursor.execute(f"DESC TABLE {qualified_landing_table}")
        rows = cursor.fetchall()
    finally:
        cursor.close()
    actual_names = tuple(str(row[0]).upper() for row in rows)
    expected_names = expected_columns(contract)
    if actual_names != expected_names:
        raise AssertionError(
            f"Landing schema columns differ for {contract.dataset}: "
            f"expected={expected_names}, actual={actual_names}"
        )
    for row, expected in zip(rows, contract.columns, strict=True):
        actual_type = str(row[1]).upper()
        expected_family = expected.snowflake_type.split("(", 1)[0].upper()
        if not actual_type.startswith(expected_family):
            raise AssertionError(
                f"Landing type differs for {contract.dataset}.{expected.name}: "
                f"expected={expected.snowflake_type}, actual={actual_type}"
            )
