# Databricks notebook source
"""Configurable Databricks widget adapter for Phase 7 secret references."""

from __future__ import annotations

from typing import Any

try:
    from notebooks.utilities.snowflake_common import DEFAULT_SECRET_SCOPE, SnowflakeSecretNames
    from notebooks.utilities.snowflake_runtime import SnowflakeCredentials, load_credentials
except ModuleNotFoundError:
    # Databricks %run executes prerequisite utilities in the notebook namespace.
    pass


def credentials_from_widgets(dbutils: Any) -> SnowflakeCredentials:
    """Create non-secret widgets and retrieve values from the configured secret scope."""
    defaults = SnowflakeSecretNames()
    widget_defaults = {
        "snowflake_secret_scope": DEFAULT_SECRET_SCOPE,
        "snowflake_organization": "",
        "snowflake_private_key_secret": defaults.private_key,
        "snowflake_account_secret": defaults.account,
        "snowflake_user_secret": defaults.user,
        "snowflake_database_secret": defaults.database,
        "snowflake_schema_secret": defaults.analytics_schema,
        "snowflake_warehouse_secret": defaults.warehouse,
        "snowflake_role_secret": defaults.role,
    }
    for name, default in widget_defaults.items():
        dbutils.widgets.text(name, default)
    names = SnowflakeSecretNames(
        private_key=dbutils.widgets.get("snowflake_private_key_secret"),
        account=dbutils.widgets.get("snowflake_account_secret"),
        user=dbutils.widgets.get("snowflake_user_secret"),
        database=dbutils.widgets.get("snowflake_database_secret"),
        analytics_schema=dbutils.widgets.get("snowflake_schema_secret"),
        warehouse=dbutils.widgets.get("snowflake_warehouse_secret"),
        role=dbutils.widgets.get("snowflake_role_secret"),
    )
    return load_credentials(
        dbutils,
        scope=dbutils.widgets.get("snowflake_secret_scope"),
        names=names,
        organization=dbutils.widgets.get("snowflake_organization") or None,
    )
