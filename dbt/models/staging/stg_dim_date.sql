select
    DATE_KEY as date_key,
    CALENDAR_DATE as calendar_date,
    YEAR as year,
    QUARTER as quarter,
    MONTH as month,
    MONTH_NAME as month_name,
    WEEK as week,
    DAY as day,
    DAY_OF_WEEK as day_of_week,
    DAY_NAME as day_name,
    IS_WEEKEND as is_weekend,
    GOLD_RUN_ID as gold_run_id,
    GOLD_PROCESSED_AT_UTC as gold_processed_at_utc
from {{ source('urbanflow_analytics', 'dim_date') }}
