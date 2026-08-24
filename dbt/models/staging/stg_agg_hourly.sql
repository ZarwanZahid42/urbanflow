select
    SOURCE_YEAR as source_year,
    SOURCE_MONTH as source_month,
    PICKUP_DATE_KEY as pickup_date_key,
    HOUR as hour,
    HOUR_BUCKET as hour_bucket,
    TIME_OF_DAY as time_of_day,
    TRIP_COUNT as trip_count,
    TOTAL_REVENUE as total_revenue,
    AVERAGE_TOTAL_AMOUNT as average_total_amount,
    AVERAGE_TRIP_DISTANCE as average_trip_distance,
    TOTAL_DISTANCE as total_distance,
    GOLD_RUN_ID as gold_run_id,
    GOLD_PROCESSED_AT_UTC as gold_processed_at_utc
from {{ source('urbanflow_analytics', 'agg_hourly') }}
