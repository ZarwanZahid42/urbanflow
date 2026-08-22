# Databricks notebook source
"""Spark transformations for UrbanFlow Silver trips and taxi zones."""

from __future__ import annotations

from typing import Any


def _source_column(dataframe: Any, requested_name: str, spark_functions: Any) -> Any:
    lookup = {name.casefold(): name for name in dataframe.columns}
    actual = lookup.get(requested_name.casefold())
    return spark_functions.col(actual) if actual else spark_functions.lit(None)


def standardize_trip_columns(dataframe: Any) -> Any:
    from pyspark.sql import functions as F

    columns = [
        _source_column(dataframe, source, F).cast(data_type).alias(target)
        for source, target, data_type in TRIP_COLUMN_SPECS
    ]
    columns.append(
        F.to_json(F.struct(*[F.col(column) for column in dataframe.columns])).alias(
            "bronze_record_json"
        )
    )
    invalid_casts = []
    for source, target, data_type in TRIP_COLUMN_SPECS:
        if target in MONEY_COLUMNS:
            raw = _source_column(dataframe, source, F)
            invalid_casts.append(raw.isNotNull() & raw.cast(data_type).isNull())
    invalid_monetary = invalid_casts[0]
    for condition in invalid_casts[1:]:
        invalid_monetary = invalid_monetary | condition
    return (
        dataframe.select(*columns)
        .withColumn("store_and_forward_flag", F.upper(F.trim(F.col("store_and_forward_flag"))))
        .withColumn("_invalid_monetary_cast", invalid_monetary)
    )


def standardize_zone_columns(dataframe: Any) -> Any:
    from pyspark.sql import functions as F

    columns = [
        _source_column(dataframe, source, F).cast(data_type).alias(target)
        for source, target, data_type in ZONE_COLUMN_SPECS
    ]
    columns.append(
        F.to_json(F.struct(*[F.col(column) for column in dataframe.columns])).alias(
            "bronze_record_json"
        )
    )
    standardized = dataframe.select(*columns)
    return (
        standardized.withColumn("borough", F.trim(F.col("borough")))
        .withColumn("zone", F.trim(F.col("zone")))
        .withColumn("service_zone", F.trim(F.col("service_zone")))
    )


def _rule_array(conditions: list[tuple[str, Any]]) -> Any:
    from pyspark.sql import functions as F

    values = F.array(*[F.when(condition, F.lit(rule)) for rule, condition in conditions])
    return F.filter(values, lambda value: value.isNotNull())


def build_zone_frames(dataframe: Any, run_id: str) -> tuple[Any, Any]:
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    standardized = standardize_zone_columns(dataframe)
    window = Window.partitionBy("location_id").orderBy(
        F.col("source_file").asc_nulls_last(),
        F.col("bronze_run_id").asc_nulls_last(),
    )
    ranked = standardized.withColumn("_duplicate_rank", F.row_number().over(window))
    rules = _rule_array(
        [
            ("NULL_LOCATION_ID", F.col("location_id").isNull()),
            ("MISSING_BOROUGH", F.trim(F.coalesce(F.col("borough"), F.lit(""))) == ""),
            ("MISSING_ZONE_NAME", F.trim(F.coalesce(F.col("zone"), F.lit(""))) == ""),
            ("DUPLICATE_LOCATION_ID", F.col("_duplicate_rank") > 1),
        ]
    )
    classified = (
        ranked.withColumn("rejection_rules", rules)
        .withColumn("rejection_rule", F.element_at("rejection_rules", 1))
        .withColumn("rejection_reason", F.concat_ws("|", "rejection_rules"))
        .withColumn("silver_run_id", F.lit(run_id))
        .withColumn("silver_processed_at_utc", F.current_timestamp())
    )
    valid = classified.where(F.size("rejection_rules") == 0).drop(
        "rejection_rules",
        "rejection_rule",
        "rejection_reason",
        "_duplicate_rank",
        "bronze_record_json",
    )
    rejected = classified.where(F.size("rejection_rules") > 0).withColumn(
        "rejected_at_utc", F.current_timestamp()
    ).drop("_duplicate_rank")
    return valid, rejected


def _trip_id_expression() -> Any:
    from pyspark.sql import functions as F

    canonical = [
        F.coalesce(F.col(column).cast("string"), F.lit("<NULL>")) for column in TRIP_KEY_COLUMNS
    ]
    return F.sha2(F.concat_ws("\u001f", *canonical), 256)


def build_trip_frames(dataframe: Any, valid_zones: Any, run_id: str) -> tuple[Any, Any]:
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    standardized = standardize_trip_columns(dataframe).withColumn("trip_id", _trip_id_expression())
    pickup_zones = valid_zones.select(
        F.col("location_id").alias("_pickup_zone_id"), F.lit(True).alias("_pickup_zone_valid")
    )
    dropoff_zones = valid_zones.select(
        F.col("location_id").alias("_dropoff_zone_id"), F.lit(True).alias("_dropoff_zone_valid")
    )
    joined = standardized.join(
        pickup_zones,
        standardized.pickup_location_id == pickup_zones._pickup_zone_id,
        "left",
    ).join(
        dropoff_zones,
        standardized.dropoff_location_id == dropoff_zones._dropoff_zone_id,
        "left",
    )
    window = Window.partitionBy("source_year", "source_month", "trip_id").orderBy(
        F.col("source_file").asc_nulls_last(),
        F.col("bronze_run_id").asc_nulls_last(),
        F.col("ingested_at_utc").asc_nulls_last(),
    )
    ranked = joined.withColumn("_duplicate_rank", F.row_number().over(window))
    negative_amount = F.lit(False)
    for column in MONEY_COLUMNS:
        negative_amount = negative_amount | (F.col(column) < 0)
    rules = _rule_array(
        [
            ("NULL_PICKUP_TIMESTAMP", F.col("pickup_datetime").isNull()),
            ("NULL_DROPOFF_TIMESTAMP", F.col("dropoff_datetime").isNull()),
            (
                "DROPOFF_BEFORE_PICKUP",
                F.col("dropoff_datetime") < F.col("pickup_datetime"),
            ),
            ("NULL_PICKUP_LOCATION", F.col("pickup_location_id").isNull()),
            ("NULL_DROPOFF_LOCATION", F.col("dropoff_location_id").isNull()),
            ("NEGATIVE_PASSENGER_COUNT", F.col("passenger_count") < 0),
            ("NULL_TRIP_DISTANCE", F.col("trip_distance").isNull()),
            (
                "INVALID_DISTANCE",
                F.isnan(F.col("trip_distance"))
                | F.col("trip_distance").isin(float("inf"), float("-inf")),
            ),
            ("NEGATIVE_DISTANCE", F.col("trip_distance") < 0),
            ("NULL_FARE_AMOUNT", F.col("fare_amount").isNull()),
            ("NULL_TOTAL_AMOUNT", F.col("total_amount").isNull()),
            ("INVALID_MONETARY_VALUE", F.col("_invalid_monetary_cast")),
            (
                "INVALID_PICKUP_LOCATION",
                F.col("pickup_location_id").isNotNull() & F.col("_pickup_zone_valid").isNull(),
            ),
            (
                "INVALID_DROPOFF_LOCATION",
                F.col("dropoff_location_id").isNotNull() & F.col("_dropoff_zone_valid").isNull(),
            ),
            ("DUPLICATE_TRIP", F.col("_duplicate_rank") > 1),
        ]
    )
    classified = (
        ranked.withColumn("is_financial_adjustment", negative_amount)
        .withColumn("rejection_rules", rules)
        .withColumn("rejection_rule", F.element_at("rejection_rules", 1))
        .withColumn("rejection_reason", F.concat_ws("|", "rejection_rules"))
        .withColumn("silver_run_id", F.lit(run_id))
        .withColumn("silver_processed_at_utc", F.current_timestamp())
    )
    internal = (
        "_invalid_monetary_cast",
        "_pickup_zone_id",
        "_pickup_zone_valid",
        "_dropoff_zone_id",
        "_dropoff_zone_valid",
        "_duplicate_rank",
    )
    valid = classified.where(F.size("rejection_rules") == 0).drop(
        "rejection_rules", "rejection_rule", "rejection_reason", "bronze_record_json", *internal
    )
    rejected = classified.where(F.size("rejection_rules") > 0).withColumn(
        "rejected_at_utc", F.current_timestamp()
    ).drop(*internal)
    return valid, rejected


def write_silver_partition(dataframe: Any, path: str, year: int, month: int) -> None:
    (
        dataframe.write.format("delta")
        .mode("overwrite")
        .option("replaceWhere", silver_replace_where(year, month))
        .option("mergeSchema", "true")
        .partitionBy("source_year", "source_month")
        .save(path)
    )


def write_silver_snapshot(dataframe: Any, path: str) -> None:
    dataframe.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(
        path
    )
