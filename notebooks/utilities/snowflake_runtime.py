# Databricks notebook source
"""Databricks runtime adapters for Phase 7.

All secret access and connector-specific behavior is intentionally isolated here.
The module is importable in local tests without PySpark or Snowflake installed.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Iterable, Mapping, Sequence

try:
    from notebooks.utilities.snowflake_common import (
        DEFAULT_SECRET_SCOPE,
        SNOWFLAKE_SPARK_FORMAT,
        SnowflakeAuditRecord,
        SnowflakeConfig,
        SnowflakeSecretNames,
        TableContract,
        landing_null_defaults,
        qualified_table,
        snowflake_spark_options,
    )
except ModuleNotFoundError:
    # Databricks %run executes this source after snowflake_common in one namespace.
    pass


@dataclass(frozen=True)
class SnowflakeCredentials:
    config: SnowflakeConfig
    private_key_pem: str

    def __repr__(self) -> str:
        return f"SnowflakeCredentials(config={self.config!r}, private_key_pem=<redacted>)"


def _secret(dbutils: Any, scope: str, key: str) -> str:
    try:
        value = dbutils.secrets.get(scope=scope, key=key)
    except Exception as exc:
        raise RuntimeError(
            f"Missing Databricks secret '{scope}/{key}'. MANUAL ACTION REQUIRED: "
            "create/populate the configured Phase 7 secret scope before live loading."
        ) from exc
    if not value or not value.strip():
        raise RuntimeError(f"Databricks secret '{scope}/{key}' is empty")
    return value


def load_credentials(
    dbutils: Any,
    *,
    scope: str = DEFAULT_SECRET_SCOPE,
    names: SnowflakeSecretNames | None = None,
    landing_schema: str = "LANDING",
    audit_schema: str = "AUDIT",
    organization: str | None = None,
) -> SnowflakeCredentials:
    names = names or SnowflakeSecretNames()
    names.validate()
    values = {
        field: _secret(dbutils, scope, secret_name)
        for field, secret_name in {
            "private_key": names.private_key,
            "account": names.account,
            "user": names.user,
            "database": names.database,
            "analytics_schema": names.analytics_schema,
            "warehouse": names.warehouse,
            "role": names.role,
        }.items()
    }
    config = SnowflakeConfig(
        account=values["account"],
        user=values["user"],
        organization=organization,
        database=values["database"],
        analytics_schema=values["analytics_schema"],
        landing_schema=landing_schema,
        audit_schema=audit_schema,
        warehouse=values["warehouse"],
        role=values["role"],
    )
    config.validate()
    return SnowflakeCredentials(config=config, private_key_pem=values["private_key"])


def open_control_connection(credentials: SnowflakeCredentials) -> Any:
    """Open a key-pair Snowflake Python connection for transactional control SQL."""
    try:
        import snowflake.connector
        from cryptography.hazmat.primitives import serialization
    except ImportError as exc:
        raise RuntimeError(
            "snowflake-connector-python is required in the Databricks Serverless environment"
        ) from exc
    try:
        key = serialization.load_pem_private_key(
            credentials.private_key_pem.encode("utf-8"), password=None
        )
        private_key_der = key.private_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        )
    except (TypeError, ValueError) as exc:
        raise RuntimeError("The configured Snowflake private key secret is not valid PEM") from exc
    config = credentials.config
    return snowflake.connector.connect(
        account=config.account_identifier,
        user=config.user,
        private_key=private_key_der,
        database=config.database,
        warehouse=config.warehouse,
        role=config.role,
        session_parameters={"QUERY_TAG": "urbanflow_phase7"},
    )


def execute(connection: Any, statement: str, parameters: Sequence[Any] | None = None) -> None:
    cursor = connection.cursor()
    try:
        cursor.execute(statement, parameters or ())
    finally:
        cursor.close()


def query_one(
    connection: Any, statement: str, parameters: Sequence[Any] | None = None
) -> Mapping[str, Any]:
    cursor = connection.cursor()
    try:
        cursor.execute(statement, parameters or ())
        row = cursor.fetchone()
        if row is None:
            raise RuntimeError("Snowflake query returned no rows")
        columns = [description[0].lower() for description in cursor.description]
        return dict(zip(columns, row, strict=True))
    finally:
        cursor.close()


def execute_transaction(connection: Any, statements: Iterable[str]) -> None:
    """Execute an explicit transaction and guarantee rollback on a failed target update."""
    cursor = connection.cursor()
    try:
        for statement in statements:
            cursor.execute(statement)
    except Exception:
        connection.rollback()
        raise
    finally:
        cursor.close()


def prepare_landing_frame(
    frame: Any,
    contract: TableContract,
    spark_functions: Any | None = None,
) -> Any:
    """Select exact Gold columns and normalize names for unquoted Snowflake identifiers."""
    if spark_functions is None:
        from pyspark.sql import functions as spark_functions

    missing = sorted(set(contract.column_names) - set(frame.columns))
    if missing:
        raise ValueError(f"{contract.dataset} is missing Gold columns: {missing}")
    defaults = landing_null_defaults(contract)
    expressions = []
    for name in contract.column_names:
        expression = spark_functions.col(name)
        if name in defaults:
            expression = spark_functions.coalesce(
                expression, spark_functions.lit(defaults[name])
            )
        expressions.append(expression.alias(name.upper()))
    return frame.select(*expressions)


def write_landing_frame(
    frame: Any,
    credentials: SnowflakeCredentials,
    contract: TableContract,
) -> None:
    options = snowflake_spark_options(
        credentials.config,
        credentials.private_key_pem,
        schema=credentials.config.landing_schema,
    )
    (
        prepare_landing_frame(frame, contract)
        .write.format(SNOWFLAKE_SPARK_FORMAT)
        .options(**options)
        .option("dbtable", contract.landing_table)
        .mode("append")
        .save()
    )


def clear_landing(connection: Any, config: SnowflakeConfig, contract: TableContract) -> None:
    table = qualified_table(config, config.landing_schema, contract.landing_table)
    execute(connection, f"DELETE FROM {table}")
    connection.commit()


def table_count(
    connection: Any,
    table: str,
    *,
    source_year: int | None = None,
    source_month: int | None = None,
) -> int:
    predicate = ""
    if source_year is not None or source_month is not None:
        if source_year is None or source_month is None:
            raise ValueError("Both source_year and source_month are required")
        predicate = f" WHERE SOURCE_YEAR = {int(source_year)} AND SOURCE_MONTH = {int(source_month)}"
    return int(query_one(connection, f"SELECT COUNT(*) AS ROW_COUNT FROM {table}{predicate}")["row_count"])


def duplicate_key_count(connection: Any, table: str, keys: Sequence[str]) -> int:
    key_sql = ", ".join(keys)
    statement = (
        "SELECT COUNT(*) AS DUPLICATE_COUNT FROM ("
        f"SELECT {key_sql} FROM {table} GROUP BY {key_sql} HAVING COUNT(*) > 1)"
    )
    return int(query_one(connection, statement)["duplicate_count"])


def append_audit(connection: Any, config: SnowflakeConfig, record: SnowflakeAuditRecord) -> None:
    row = record.as_row()
    table = qualified_table(config, config.audit_schema, "LOAD_AUDIT")
    columns = ", ".join(name.upper() for name in row)
    placeholders = ", ".join(["%s"] * len(row))
    execute(
        connection,
        f"INSERT INTO {table} ({columns}) VALUES ({placeholders})",
        tuple(row.values()),
    )
    connection.commit()


def _sanitized_error(error: Exception) -> str:
    message = f"{type(error).__name__}: {error}"
    message = re.sub(
        r"-----BEGIN [^-]*PRIVATE KEY-----.*?-----END [^-]*PRIVATE KEY-----",
        "<redacted-private-key>",
        message,
        flags=re.DOTALL,
    )
    return message[:4000]


def failed_audit(
    *,
    run_id: str,
    dataset: str,
    started_at: datetime,
    error: Exception,
    source_year: int | None,
    source_month: int | None,
) -> SnowflakeAuditRecord:
    return SnowflakeAuditRecord(
        run_id=run_id,
        dataset=dataset,
        source_year=source_year,
        source_month=source_month,
        source_row_count=None,
        landing_row_count=None,
        target_row_count=None,
        status="FAILED",
        started_at=started_at,
        completed_at=datetime.now(UTC),
        error_message=_sanitized_error(error),
        reconciliation_status="FAILED",
    )
