select
    source_year,
    source_month,
    pickup_date_key,
    trip_count,
    financial_adjustment_count
from {{ ref('mart_daily_mobility') }}
where
    trip_count < 0
    or financial_adjustment_count < 0
    or financial_adjustment_count > trip_count
