select
    SOURCE_YEAR as source_year,
    SOURCE_MONTH as source_month,
    LOCATION_ID as location_id,
    BOROUGH as borough,
    ZONE as zone,
    PICKUP_TRIP_COUNT as pickup_trip_count,
    DROPOFF_TRIP_COUNT as dropoff_trip_count,
    TOTAL_REVENUE as total_revenue,
    AVERAGE_TRIP_DISTANCE as average_trip_distance,
    AVERAGE_TOTAL_AMOUNT as average_total_amount,
    TOTAL_DISTANCE as total_distance,
    GOLD_RUN_ID as gold_run_id,
    GOLD_PROCESSED_AT_UTC as gold_processed_at_utc
from {{ source('urbanflow_analytics', 'agg_location') }}
