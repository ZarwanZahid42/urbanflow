# Databricks notebook source
"""Pure-Python contracts and plans for the UrbanFlow Snowflake integration."""

from __future__ import annotations

import re
from dataclasses import asdict, dataclass, field
from datetime import datetime
from typing import Any, Iterable, Mapping

SNOWFLAKE_SPARK_FORMAT = "snowflake"
DEFAULT_SECRET_SCOPE = "urbanflow-snowflake"
PHASE7_SCHEMA_VERSION = "phase7-v1"

_IDENTIFIER = re.compile(r"^[A-Z][A-Z0-9_]*$")


@dataclass(frozen=True)
class SnowflakeSecretNames:
    private_key: str = "snowflake_private_key"
    account: str = "snowflake_account"
    user: str = "snowflake_user"
    database: str = "snowflake_database"
    analytics_schema: str = "snowflake_schema"
    warehouse: str = "snowflake_warehouse"
    role: str = "snowflake_role"

    def validate(self) -> None:
        for value in asdict(self).values():
            if not value or any(character.isspace() for character in value):
                raise ValueError("Snowflake secret names must be non-empty and contain no spaces")


@dataclass(frozen=True)
class SnowflakeConfig:
    account: str
    user: str
    organization: str | None = None
    database: str = "URBANFLOW"
    analytics_schema: str = "ANALYTICS"
    landing_schema: str = "LANDING"
    audit_schema: str = "AUDIT"
    warehouse: str = "URBANFLOW_LOAD_WH"
    role: str = "URBANFLOW_LOADER_ROLE"

    @property
    def account_identifier(self) -> str:
        account = self.account.strip().lower().removesuffix(".snowflakecomputing.com")
        organization = (self.organization or "").strip().lower()
        if organization and "-" not in account and "." not in account:
            return f"{organization}-{account}"
        return account

    @property
    def host(self) -> str:
        return f"{self.account_identifier}.snowflakecomputing.com"

    def validate(self) -> None:
        account = self.account.strip().lower().removesuffix(".snowflakecomputing.com")
        if not account or not re.fullmatch(r"[a-z0-9_-]+(?:\.[a-z0-9_-]+)*", account):
            raise ValueError("Snowflake account must be an account identifier, not a URL")
        if self.organization and not re.fullmatch(r"[A-Za-z0-9_]+", self.organization.strip()):
            raise ValueError("Invalid Snowflake organization")
        for label in (
            "user",
            "database",
            "analytics_schema",
            "landing_schema",
            "audit_schema",
            "warehouse",
            "role",
        ):
            value = getattr(self, label).strip().upper()
            if not _IDENTIFIER.fullmatch(value):
                raise ValueError(f"Invalid Snowflake {label}: {getattr(self, label)!r}")
        schemas = {
            self.analytics_schema.strip().upper(),
            self.landing_schema.strip().upper(),
            self.audit_schema.strip().upper(),
        }
        if len(schemas) != 3:
            raise ValueError("Snowflake analytics, landing, and audit schemas must be distinct")


def snowflake_spark_options(
    config: SnowflakeConfig,
    private_key_pem: str,
    *,
    schema: str,
) -> dict[str, str]:
    """Build only supported Serverless Snowflake connector options."""
    config.validate()
    private_key = normalize_snowflake_spark_private_key(private_key_pem)
    schema_name = schema.strip().upper()
    if schema_name not in {
        config.analytics_schema.upper(),
        config.landing_schema.upper(),
        config.audit_schema.upper(),
    }:
        raise ValueError(f"Schema is outside the configured UrbanFlow schemas: {schema!r}")
    return {
        "host": config.host,
        "sfaccount": config.account_identifier,
        "sfuser": config.user,
        "sfauthenticator": "snowflake_jwt",
        "pem_private_key": private_key,
        "sfdatabase": config.database,
        "sfschema": schema_name,
        "sfwarehouse": config.warehouse,
        "sfrole": config.role,
        "column_mapping": "name",
        "column_mismatch_behavior": "error",
        "usestagingtable": "true",
    }


def normalize_snowflake_spark_private_key(private_key_pem: str) -> str:
    """Return the unencrypted PKCS#8 RSA key payload expected by the Spark connector."""
    if not isinstance(private_key_pem, str) or not private_key_pem.strip():
        raise ValueError("Snowflake private key secret is empty")

    normalized = private_key_pem.strip()
    normalized = normalized.replace("\\r\\n", "\n").replace("\\n", "\n").replace("\\r", "\n")
    normalized = normalized.replace("\r\n", "\n").replace("\r", "\n").strip()

    if "-----BEGIN ENCRYPTED PRIVATE KEY-----" in normalized:
        raise ValueError("Snowflake Spark authentication requires an unencrypted private key")
    if "-----BEGIN RSA PRIVATE KEY-----" in normalized:
        raise ValueError("Snowflake Spark authentication requires PKCS#8, not RSA PKCS#1")
    if not (
        normalized.startswith("-----BEGIN PRIVATE KEY-----\n")
        and normalized.endswith("\n-----END PRIVATE KEY-----")
    ):
        raise ValueError("Snowflake private key must be an unencrypted PKCS#8 PEM")

    try:
        from cryptography.hazmat.primitives import serialization
        from cryptography.hazmat.primitives.asymmetric import rsa

        key = serialization.load_pem_private_key(normalized.encode("utf-8"), password=None)
    except (TypeError, ValueError) as exc:
        raise ValueError("Snowflake private key must be a valid unencrypted PKCS#8 PEM") from exc
    if not isinstance(key, rsa.RSAPrivateKey):
        raise ValueError("Snowflake private key must contain an RSA private key")

    canonical_pem = key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    ).decode("ascii")
    return re.sub(
        r"\s+",
        "",
        canonical_pem.removeprefix("-----BEGIN PRIVATE KEY-----").removesuffix(
            "-----END PRIVATE KEY-----\n"
        ),
    )


@dataclass(frozen=True)
class ColumnContract:
    name: str
    snowflake_type: str
    nullable: bool = True


@dataclass(frozen=True)
class TableContract:
    dataset: str
    gold_relative_path: str
    columns: tuple[ColumnContract, ...]
    key_columns: tuple[str, ...]
    replacement: str
    landing_table: str
    analytics_table: str
    partition_columns: tuple[str, ...] = ()

    @property
    def column_names(self) -> tuple[str, ...]:
        return tuple(column.name for column in self.columns)

    def validate(self) -> None:
        names = self.column_names
        if len(names) != len(set(names)):
            raise ValueError(f"Duplicate columns in {self.dataset}")
        if not set(self.key_columns).issubset(names):
            raise ValueError(f"Missing key column in {self.dataset}")
        if not set(self.partition_columns).issubset(names):
            raise ValueError(f"Missing partition column in {self.dataset}")
        if self.replacement not in {"PARTITION", "SNAPSHOT"}:
            raise ValueError(f"Invalid replacement strategy for {self.dataset}")


def _columns(specification: str) -> tuple[ColumnContract, ...]:
    output = []
    for item in specification.split("|"):
        name, snowflake_type, nullable = item.split(":")
        output.append(ColumnContract(name, snowflake_type, nullable == "Y"))
    return tuple(output)


_FACT_COLUMNS = _columns(
    "trip_id:VARCHAR:N|vendor_id:NUMBER(10,0):Y|pickup_datetime:TIMESTAMP_NTZ:N|"
    "dropoff_datetime:TIMESTAMP_NTZ:N|pickup_date_key:NUMBER(10,0):N|"
    "dropoff_date_key:NUMBER(10,0):N|pickup_time_key:NUMBER(10,0):N|"
    "dropoff_time_key:NUMBER(10,0):N|pickup_location_id:NUMBER(10,0):N|"
    "dropoff_location_id:NUMBER(10,0):N|passenger_count:NUMBER(10,2):Y|"
    "trip_distance:FLOAT:Y|rate_code_id:NUMBER(10,0):Y|payment_type:NUMBER(10,0):Y|"
    "store_and_forward_flag:VARCHAR:Y|fare_amount:NUMBER(18,2):Y|extra:NUMBER(18,2):Y|"
    "mta_tax:NUMBER(18,2):Y|tip_amount:NUMBER(18,2):Y|tolls_amount:NUMBER(18,2):Y|"
    "improvement_surcharge:NUMBER(18,2):Y|congestion_surcharge:NUMBER(18,2):Y|"
    "airport_fee:NUMBER(18,2):Y|cbd_congestion_fee:NUMBER(18,2):Y|"
    "total_amount:NUMBER(18,2):Y|trip_duration_minutes:FLOAT:Y|average_speed_mph:FLOAT:Y|"
    "fare_per_mile:FLOAT:Y|tip_percentage:FLOAT:Y|is_financial_adjustment:BOOLEAN:N|"
    "non_adjustment_revenue:NUMBER(18,2):Y|financial_adjustment_amount:NUMBER(18,2):Y|"
    "source_year:NUMBER(10,0):N|source_month:NUMBER(10,0):N|source_file:VARCHAR:Y|"
    "ingested_at_utc:TIMESTAMP_NTZ:Y|bronze_run_id:VARCHAR:Y|silver_run_id:VARCHAR:Y|"
    "silver_processed_at_utc:TIMESTAMP_NTZ:Y|gold_run_id:VARCHAR:N|"
    "gold_processed_at_utc:TIMESTAMP_NTZ:N"
)

_DATE_COLUMNS = _columns(
    "date_key:NUMBER(10,0):N|calendar_date:DATE:N|year:NUMBER(10,0):N|"
    "quarter:NUMBER(10,0):N|month:NUMBER(10,0):N|month_name:VARCHAR:N|"
    "week:NUMBER(10,0):N|day:NUMBER(10,0):N|day_of_week:NUMBER(10,0):N|"
    "day_name:VARCHAR:N|is_weekend:BOOLEAN:N|gold_run_id:VARCHAR:N|"
    "gold_processed_at_utc:TIMESTAMP_NTZ:N"
)

_TIME_COLUMNS = _columns(
    "time_key:NUMBER(10,0):N|hour:NUMBER(10,0):N|minute:NUMBER(10,0):N|"
    "hour_bucket:VARCHAR:N|am_pm:VARCHAR:N|time_of_day:VARCHAR:N|gold_run_id:VARCHAR:N|"
    "gold_processed_at_utc:TIMESTAMP_NTZ:N"
)

_LOCATION_COLUMNS = _columns(
    "location_id:NUMBER(10,0):N|borough:VARCHAR:Y|zone:VARCHAR:Y|service_zone:VARCHAR:Y|"
    "borough_normalized:VARCHAR:Y|zone_normalized:VARCHAR:Y|source_file:VARCHAR:Y|"
    "ingested_at_utc:TIMESTAMP_NTZ:Y|bronze_run_id:VARCHAR:Y|silver_run_id:VARCHAR:Y|"
    "silver_processed_at_utc:TIMESTAMP_NTZ:Y|gold_run_id:VARCHAR:N|"
    "gold_processed_at_utc:TIMESTAMP_NTZ:N"
)

_DAILY_COLUMNS = _columns(
    "source_year:NUMBER(10,0):N|source_month:NUMBER(10,0):N|pickup_date_key:NUMBER(10,0):N|"
    "trip_count:NUMBER(19,0):N|total_revenue:NUMBER(28,2):Y|average_fare:NUMBER(18,2):Y|"
    "average_total_amount:NUMBER(18,2):Y|average_trip_distance:FLOAT:Y|"
    "total_distance:FLOAT:Y|average_passenger_count:NUMBER(18,2):Y|"
    "tip_revenue:NUMBER(28,2):Y|toll_revenue:NUMBER(28,2):Y|"
    "non_adjustment_revenue:NUMBER(28,2):Y|financial_adjustment_count:NUMBER(19,0):N|"
    "financial_adjustment_amount:NUMBER(28,2):Y|gold_run_id:VARCHAR:N|"
    "gold_processed_at_utc:TIMESTAMP_NTZ:N"
)

_LOCATION_AGG_COLUMNS = _columns(
    "source_year:NUMBER(10,0):N|source_month:NUMBER(10,0):N|location_id:NUMBER(10,0):N|"
    "borough:VARCHAR:Y|zone:VARCHAR:Y|pickup_trip_count:NUMBER(19,0):N|"
    "dropoff_trip_count:NUMBER(19,0):N|total_revenue:NUMBER(28,2):Y|"
    "average_trip_distance:FLOAT:Y|average_total_amount:NUMBER(18,2):Y|"
    "total_distance:FLOAT:Y|gold_run_id:VARCHAR:N|gold_processed_at_utc:TIMESTAMP_NTZ:N"
)

_HOURLY_COLUMNS = _columns(
    "source_year:NUMBER(10,0):N|source_month:NUMBER(10,0):N|pickup_date_key:NUMBER(10,0):N|"
    "hour:NUMBER(10,0):N|hour_bucket:VARCHAR:N|time_of_day:VARCHAR:N|"
    "trip_count:NUMBER(19,0):N|total_revenue:NUMBER(28,2):Y|"
    "average_total_amount:NUMBER(18,2):Y|average_trip_distance:FLOAT:Y|"
    "total_distance:FLOAT:Y|gold_run_id:VARCHAR:N|gold_processed_at_utc:TIMESTAMP_NTZ:N"
)

TABLE_CONTRACTS: tuple[TableContract, ...] = (
    TableContract("fact_trips", "gold/fact_trips", _FACT_COLUMNS, ("trip_id",), "PARTITION", "FACT_TRIPS", "FACT_TRIPS", ("source_year", "source_month")),
    TableContract("dim_date", "gold/dim_date", _DATE_COLUMNS, ("date_key",), "SNAPSHOT", "DIM_DATE", "DIM_DATE"),
    TableContract("dim_time", "gold/dim_time", _TIME_COLUMNS, ("time_key",), "SNAPSHOT", "DIM_TIME", "DIM_TIME"),
    TableContract("dim_location", "gold/dim_location", _LOCATION_COLUMNS, ("location_id",), "SNAPSHOT", "DIM_LOCATION", "DIM_LOCATION"),
    TableContract("agg_daily_trips", "gold/agg_daily_trips", _DAILY_COLUMNS, ("source_year", "source_month", "pickup_date_key"), "PARTITION", "AGG_DAILY_TRIPS", "AGG_DAILY_TRIPS", ("source_year", "source_month")),
    TableContract("agg_location_trips", "gold/agg_location_trips", _LOCATION_AGG_COLUMNS, ("source_year", "source_month", "location_id"), "PARTITION", "AGG_LOCATION_TRIPS", "AGG_LOCATION_TRIPS", ("source_year", "source_month")),
    TableContract("agg_hourly_trips", "gold/agg_hourly_trips", _HOURLY_COLUMNS, ("source_year", "source_month", "pickup_date_key", "hour"), "PARTITION", "AGG_HOURLY_TRIPS", "AGG_HOURLY_TRIPS", ("source_year", "source_month")),
)


def table_contract(dataset: str) -> TableContract:
    for contract in TABLE_CONTRACTS:
        if contract.dataset == dataset:
            contract.validate()
            return contract
    raise KeyError(f"Unknown Snowflake dataset: {dataset}")


def landing_null_defaults(contract: TableContract) -> dict[str, Any]:
    """Return intentional null defaults applied only at the Snowflake landing boundary."""
    contract.validate()
    if contract.dataset == "fact_trips":
        return {"is_financial_adjustment": False}
    return {}


def validate_batch(year: int, month: int) -> None:
    if year < 2009 or year > 9999 or month < 1 or month > 12:
        raise ValueError("Invalid TLC source year/month")


def qualified_table(config: SnowflakeConfig, schema: str, table: str) -> str:
    config.validate()
    identifiers = (config.database.upper(), schema.upper(), table.upper())
    if not all(_IDENTIFIER.fullmatch(identifier) for identifier in identifiers):
        raise ValueError("Unsafe Snowflake identifier")
    return ".".join(identifiers)


@dataclass(frozen=True)
class ReplacementPlan:
    dataset: str
    strategy: str
    statements: tuple[str, ...]
    source_year: int | None = None
    source_month: int | None = None


def replacement_plan(
    contract: TableContract,
    config: SnowflakeConfig,
    *,
    source_year: int | None = None,
    source_month: int | None = None,
) -> ReplacementPlan:
    contract.validate()
    landing = qualified_table(config, config.landing_schema, contract.landing_table)
    target = qualified_table(config, config.analytics_schema, contract.analytics_table)
    columns = ", ".join(column.name.upper() for column in contract.columns)
    if contract.replacement == "PARTITION":
        if source_year is None or source_month is None:
            raise ValueError("Partition replacements require source_year and source_month")
        validate_batch(source_year, source_month)
        predicate = f"SOURCE_YEAR = {source_year} AND SOURCE_MONTH = {source_month}"
    else:
        if source_year is not None or source_month is not None:
            raise ValueError("Snapshot replacements do not accept partition values")
        predicate = "1 = 1"
    return ReplacementPlan(
        dataset=contract.dataset,
        strategy=contract.replacement,
        source_year=source_year,
        source_month=source_month,
        statements=(
            "BEGIN",
            f"DELETE FROM {target} WHERE {predicate}",
            f"INSERT INTO {target} ({columns}) SELECT {columns} FROM {landing}",
            "COMMIT",
        ),
    )


@dataclass(frozen=True)
class ReconciliationResult:
    dataset: str
    source_row_count: int
    landing_row_count: int
    target_row_count: int
    duplicate_key_count: int = 0
    boundary_failure_count: int = 0
    referential_failure_count: int = 0
    aggregate_difference: float = 0.0
    status: str = field(init=False)

    def __post_init__(self) -> None:
        counts_match = self.source_row_count == self.landing_row_count == self.target_row_count
        valid = (
            counts_match
            and self.duplicate_key_count == 0
            and self.boundary_failure_count == 0
            and self.referential_failure_count == 0
            and abs(self.aggregate_difference) < 0.005
        )
        object.__setattr__(self, "status", "PASS" if valid else "FAILED")


def idempotency_status(
    first_counts: Mapping[str, int], second_counts: Mapping[str, int]
) -> str:
    if set(first_counts) != set(second_counts):
        return "FAILED"
    if any(value < 0 for value in (*first_counts.values(), *second_counts.values())):
        raise ValueError("Counts cannot be negative")
    return "PASS" if dict(first_counts) == dict(second_counts) else "FAILED"


@dataclass(frozen=True)
class SnowflakeAuditRecord:
    run_id: str
    dataset: str
    source_year: int | None
    source_month: int | None
    source_row_count: int | None
    landing_row_count: int | None
    target_row_count: int | None
    status: str
    started_at: datetime
    completed_at: datetime
    error_message: str | None
    reconciliation_status: str
    idempotency_pass: int | None = None
    idempotency_status: str | None = None
    schema_version: str = PHASE7_SCHEMA_VERSION

    def as_row(self) -> dict[str, Any]:
        if not self.run_id.strip() or not self.dataset.strip():
            raise ValueError("Audit run_id and dataset are required")
        if self.completed_at < self.started_at:
            raise ValueError("completed_at cannot precede started_at")
        if self.status not in {"STARTED", "LANDED", "SUCCEEDED", "FAILED"}:
            raise ValueError(f"Invalid audit status: {self.status}")
        if self.reconciliation_status not in {"NOT_RUN", "PASS", "FAILED"}:
            raise ValueError("Invalid reconciliation status")
        for value in (self.source_row_count, self.landing_row_count, self.target_row_count):
            if value is not None and value < 0:
                raise ValueError("Audit row counts cannot be negative")
        if self.status == "FAILED" and not self.error_message:
            raise ValueError("Failed audits require an error message")
        if self.status != "FAILED" and self.error_message:
            raise ValueError("Successful audit records cannot contain an error message")
        return asdict(self)


def expected_columns(contract: TableContract) -> tuple[str, ...]:
    return tuple(name.upper() for name in contract.column_names)


def missing_columns(actual: Iterable[str], expected: Iterable[str]) -> list[str]:
    return sorted(set(map(str.upper, expected)) - set(map(str.upper, actual)))
