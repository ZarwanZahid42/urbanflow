select
    source_year,
    source_month,
    pickup_date_key,
    hour
from {{ ref('mart_hourly_mobility') }}
group by
    source_year,
    source_month,
    pickup_date_key,
    hour
having count(*) > 1
