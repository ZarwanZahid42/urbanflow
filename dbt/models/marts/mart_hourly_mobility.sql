select
    hourly.source_year,
    hourly.source_month,
    hourly.pickup_date_key,
    date_dimension.calendar_date as pickup_date,
    date_dimension.year as pickup_year,
    date_dimension.quarter as pickup_quarter,
    date_dimension.month as pickup_calendar_month,
    date_dimension.month_name as pickup_month_name,
    date_dimension.week as pickup_week,
    date_dimension.day as pickup_day,
    date_dimension.day_of_week as pickup_day_of_week,
    date_dimension.day_name as pickup_day_name,
    date_dimension.is_weekend as pickup_is_weekend,
    hourly.hour,
    hourly.hour_bucket,
    hourly.time_of_day,
    hourly.trip_count,
    hourly.total_revenue,
    hourly.average_total_amount,
    hourly.average_trip_distance,
    hourly.total_distance,
    hourly.gold_run_id,
    hourly.gold_processed_at_utc
from {{ ref('stg_agg_hourly') }} as hourly
inner join {{ ref('stg_dim_date') }} as date_dimension
    on hourly.pickup_date_key = date_dimension.date_key
