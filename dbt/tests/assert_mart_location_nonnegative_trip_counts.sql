select
    source_year,
    source_month,
    location_id,
    pickup_trip_count,
    dropoff_trip_count
from {{ ref('mart_location_mobility') }}
where pickup_trip_count < 0 or dropoff_trip_count < 0
