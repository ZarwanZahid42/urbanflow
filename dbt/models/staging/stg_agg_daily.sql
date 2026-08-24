select
    SOURCE_YEAR as source_year,
    SOURCE_MONTH as source_month,
    PICKUP_DATE_KEY as pickup_date_key,
    TRIP_COUNT as trip_count,
    TOTAL_REVENUE as total_revenue,
    AVERAGE_FARE as average_fare,
    AVERAGE_TOTAL_AMOUNT as average_total_amount,
    AVERAGE_TRIP_DISTANCE as average_trip_distance,
    TOTAL_DISTANCE as total_distance,
    AVERAGE_PASSENGER_COUNT as average_passenger_count,
    TIP_REVENUE as tip_revenue,
    TOLL_REVENUE as toll_revenue,
    NON_ADJUSTMENT_REVENUE as non_adjustment_revenue,
    FINANCIAL_ADJUSTMENT_COUNT as financial_adjustment_count,
    FINANCIAL_ADJUSTMENT_AMOUNT as financial_adjustment_amount,
    GOLD_RUN_ID as gold_run_id,
    GOLD_PROCESSED_AT_UTC as gold_processed_at_utc
from {{ source('urbanflow_analytics', 'agg_daily') }}
