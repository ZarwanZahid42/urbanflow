select
    TIME_KEY as time_key,
    HOUR as hour,
    MINUTE as minute,
    HOUR_BUCKET as hour_bucket,
    AM_PM as am_pm,
    TIME_OF_DAY as time_of_day,
    GOLD_RUN_ID as gold_run_id,
    GOLD_PROCESSED_AT_UTC as gold_processed_at_utc
from {{ source('urbanflow_analytics', 'dim_time') }}
