# Databricks notebook source
"""Spark transformations and Delta writers for UrbanFlow Gold models."""

from __future__ import annotations

from typing import Any


def build_dim_date(silver_trips: Any, run_id: str) -> Any:
    from pyspark.sql import functions as F

    bounds = silver_trips.agg(
        F.least(F.min(F.to_date("pickup_datetime")), F.min(F.to_date("dropoff_datetime"))).alias(
            "start_date"
        ),
        F.greatest(F.max(F.to_date("pickup_datetime")), F.max(F.to_date("dropoff_datetime"))).alias(
            "end_date"
        ),
    )
    dates = bounds.select(
        F.explode(F.sequence(F.col("start_date"), F.col("end_date"))).alias("calendar_date")
    )
    return dates.select(
        F.date_format("calendar_date", "yyyyMMdd").cast("int").alias("date_key"),
        F.col("calendar_date").cast("date"),
        F.year("calendar_date").cast("int").alias("year"),
        F.quarter("calendar_date").cast("int").alias("quarter"),
        F.month("calendar_date").cast("int").alias("month"),
        F.date_format("calendar_date", "MMMM").alias("month_name"),
        F.weekofyear("calendar_date").cast("int").alias("week"),
        F.dayofmonth("calendar_date").cast("int").alias("day"),
        F.when(F.dayofweek("calendar_date") == 1, 7)
        .otherwise(F.dayofweek("calendar_date") - 1)
        .cast("int")
        .alias("day_of_week"),
        F.date_format("calendar_date", "EEEE").alias("day_name"),
        F.dayofweek("calendar_date").isin(1, 7).alias("is_weekend"),
        F.lit(run_id).alias("gold_run_id"),
        F.current_timestamp().alias("gold_processed_at_utc"),
    )


def build_dim_time(spark_session: Any, run_id: str) -> Any:
    from pyspark.sql import functions as F

    minutes = spark_session.range(0, 24 * 60).select(
        (F.col("id") / 60).cast("int").alias("hour"),
        (F.col("id") % 60).cast("int").alias("minute"),
    )
    return minutes.select(
        (F.col("hour") * 100 + F.col("minute")).cast("int").alias("time_key"),
        F.col("hour"),
        F.col("minute"),
        F.format_string("%02d:00-%02d:59", F.col("hour"), F.col("hour")).alias("hour_bucket"),
        F.when(F.col("hour") < 12, "AM").otherwise("PM").alias("am_pm"),
        F.when(F.col("hour") < 6, "Overnight")
        .when(F.col("hour") < 12, "Morning")
        .when(F.col("hour") < 18, "Afternoon")
        .otherwise("Evening")
        .alias("time_of_day"),
        F.lit(run_id).alias("gold_run_id"),
        F.current_timestamp().alias("gold_processed_at_utc"),
    )


def build_dim_location(silver_zones: Any, run_id: str) -> Any:
    from pyspark.sql import functions as F

    def normalized(column: str) -> Any:
        return F.regexp_replace(F.lower(F.trim(F.col(column))), "[^a-z0-9]+", "_")

    return silver_zones.select(
        F.col("location_id").cast("int"),
        F.trim("borough").alias("borough"),
        F.trim("zone").alias("zone"),
        F.trim("service_zone").alias("service_zone"),
        normalized("borough").alias("borough_normalized"),
        normalized("zone").alias("zone_normalized"),
        F.col("source_file"),
        F.col("ingested_at_utc"),
        F.col("bronze_run_id"),
        F.col("silver_run_id"),
        F.col("silver_processed_at_utc"),
        F.lit(run_id).alias("gold_run_id"),
        F.current_timestamp().alias("gold_processed_at_utc"),
    )


def _finite_ratio(numerator: Any, denominator: Any, valid_condition: Any) -> Any:
    from pyspark.sql import functions as F

    value = numerator.cast("double") / denominator.cast("double")
    return F.when(valid_condition & ~F.isnan(value) & ~value.isin(float("inf"), float("-inf")), value)


def build_gold_fact(silver_trips: Any, run_id: str) -> Any:
    from pyspark.sql import functions as F

    duration_minutes = (
        F.col("dropoff_datetime").cast("long") - F.col("pickup_datetime").cast("long")
    ) / F.lit(60.0)
    speed = _finite_ratio(
        F.col("trip_distance"),
        duration_minutes / F.lit(60.0),
        (duration_minutes > 0) & (F.col("trip_distance") >= 0),
    )
    fare_per_mile = _finite_ratio(
        F.col("fare_amount"),
        F.col("trip_distance"),
        F.col("trip_distance") > 0,
    )
    tip_percentage = _finite_ratio(
        F.col("tip_amount") * F.lit(100.0),
        F.col("fare_amount"),
        F.col("fare_amount") > 0,
    )
    zero_money = F.lit(0).cast("decimal(18,2)")
    return silver_trips.select(
        "trip_id",
        "vendor_id",
        "pickup_datetime",
        "dropoff_datetime",
        F.date_format("pickup_datetime", "yyyyMMdd").cast("int").alias("pickup_date_key"),
        F.date_format("dropoff_datetime", "yyyyMMdd").cast("int").alias("dropoff_date_key"),
        (F.hour("pickup_datetime") * 100 + F.minute("pickup_datetime"))
        .cast("int")
        .alias("pickup_time_key"),
        (F.hour("dropoff_datetime") * 100 + F.minute("dropoff_datetime"))
        .cast("int")
        .alias("dropoff_time_key"),
        "pickup_location_id",
        "dropoff_location_id",
        "passenger_count",
        "trip_distance",
        "rate_code_id",
        "payment_type",
        "store_and_forward_flag",
        "fare_amount",
        "extra",
        "mta_tax",
        "tip_amount",
        "tolls_amount",
        "improvement_surcharge",
        "congestion_surcharge",
        "airport_fee",
        "cbd_congestion_fee",
        "total_amount",
        duration_minutes.cast("double").alias("trip_duration_minutes"),
        speed.cast("double").alias("average_speed_mph"),
        fare_per_mile.cast("double").alias("fare_per_mile"),
        tip_percentage.cast("double").alias("tip_percentage"),
        "is_financial_adjustment",
        F.when(~F.col("is_financial_adjustment"), F.col("total_amount"))
        .otherwise(zero_money)
        .alias("non_adjustment_revenue"),
        F.when(F.col("is_financial_adjustment"), F.col("total_amount"))
        .otherwise(zero_money)
        .alias("financial_adjustment_amount"),
        "source_year",
        "source_month",
        "source_file",
        "ingested_at_utc",
        "bronze_run_id",
        "silver_run_id",
        "silver_processed_at_utc",
        F.lit(run_id).alias("gold_run_id"),
        F.current_timestamp().alias("gold_processed_at_utc"),
    )


def build_daily_aggregation(fact: Any) -> Any:
    from pyspark.sql import functions as F

    return fact.groupBy("source_year", "source_month", "pickup_date_key").agg(
        F.count("trip_id").cast("long").alias("trip_count"),
        F.sum("total_amount").cast("decimal(28,2)").alias("total_revenue"),
        F.avg("fare_amount").cast("decimal(18,2)").alias("average_fare"),
        F.avg("total_amount").cast("decimal(18,2)").alias("average_total_amount"),
        F.avg("trip_distance").cast("double").alias("average_trip_distance"),
        F.sum("trip_distance").cast("double").alias("total_distance"),
        F.avg("passenger_count").cast("decimal(18,2)").alias("average_passenger_count"),
        F.sum("tip_amount").cast("decimal(28,2)").alias("tip_revenue"),
        F.sum("tolls_amount").cast("decimal(28,2)").alias("toll_revenue"),
        F.sum("non_adjustment_revenue").cast("decimal(28,2)").alias(
            "non_adjustment_revenue"
        ),
        F.sum(F.col("is_financial_adjustment").cast("long")).alias(
            "financial_adjustment_count"
        ),
        F.sum("financial_adjustment_amount").cast("decimal(28,2)").alias(
            "financial_adjustment_amount"
        ),
    )


def build_location_aggregation(fact: Any, locations: Any) -> Any:
    from pyspark.sql import functions as F

    batches = fact.select("source_year", "source_month").distinct()
    base = batches.crossJoin(locations.select("location_id", "borough", "zone"))
    pickup = fact.groupBy("source_year", "source_month", "pickup_location_id").agg(
        F.count("trip_id").cast("long").alias("pickup_trip_count"),
        F.sum("total_amount").cast("decimal(28,2)").alias("total_revenue"),
        F.avg("trip_distance").cast("double").alias("average_trip_distance"),
        F.avg("total_amount").cast("decimal(18,2)").alias("average_total_amount"),
        F.sum("trip_distance").cast("double").alias("total_distance"),
    )
    dropoff = fact.groupBy("source_year", "source_month", "dropoff_location_id").agg(
        F.count("trip_id").cast("long").alias("dropoff_trip_count")
    )
    return (
        base.join(
            pickup,
            (base.source_year == pickup.source_year)
            & (base.source_month == pickup.source_month)
            & (base.location_id == pickup.pickup_location_id),
            "left",
        )
        .join(
            dropoff,
            (base.source_year == dropoff.source_year)
            & (base.source_month == dropoff.source_month)
            & (base.location_id == dropoff.dropoff_location_id),
            "left",
        )
        .select(
            base.source_year,
            base.source_month,
            base.location_id,
            base.borough,
            base.zone,
            F.coalesce(pickup.pickup_trip_count, F.lit(0)).cast("long").alias(
                "pickup_trip_count"
            ),
            F.coalesce(dropoff.dropoff_trip_count, F.lit(0)).cast("long").alias(
                "dropoff_trip_count"
            ),
            F.coalesce(pickup.total_revenue, F.lit(0)).cast("decimal(28,2)").alias(
                "total_revenue"
            ),
            pickup.average_trip_distance,
            pickup.average_total_amount,
            F.coalesce(pickup.total_distance, F.lit(0.0)).cast("double").alias(
                "total_distance"
            ),
        )
    )


def build_hourly_aggregation(fact: Any, time_dimension: Any) -> Any:
    from pyspark.sql import functions as F

    hourly = fact.groupBy(
        "source_year",
        "source_month",
        "pickup_date_key",
        F.hour("pickup_datetime").cast("int").alias("hour"),
    ).agg(
        F.count("trip_id").cast("long").alias("trip_count"),
        F.sum("total_amount").cast("decimal(28,2)").alias("total_revenue"),
        F.avg("total_amount").cast("decimal(18,2)").alias("average_total_amount"),
        F.avg("trip_distance").cast("double").alias("average_trip_distance"),
        F.sum("trip_distance").cast("double").alias("total_distance"),
    )
    hour_labels = time_dimension.where("minute = 0").select(
        "hour", "hour_bucket", "time_of_day"
    )
    return hourly.join(hour_labels, "hour", "inner").select(
        "source_year",
        "source_month",
        "pickup_date_key",
        "hour",
        "hour_bucket",
        "time_of_day",
        "trip_count",
        "total_revenue",
        "average_total_amount",
        "average_trip_distance",
        "total_distance",
    )


def write_gold_partition(dataframe: Any, path: str, year: int, month: int) -> None:
    (
        dataframe.write.format("delta")
        .mode("overwrite")
        .option("replaceWhere", gold_replace_where(year, month))
        .option("mergeSchema", "true")
        .partitionBy("source_year", "source_month")
        .save(path)
    )


def write_gold_snapshot(dataframe: Any, path: str) -> None:
    dataframe.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(
        path
    )
