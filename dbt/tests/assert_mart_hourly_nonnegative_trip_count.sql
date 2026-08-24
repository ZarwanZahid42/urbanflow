select
    source_year,
    source_month,
    pickup_date_key,
    hour,
    trip_count
from {{ ref('mart_hourly_mobility') }}
where trip_count < 0
