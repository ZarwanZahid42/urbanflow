select
    source_year,
    source_month,
    pickup_date_key
from {{ ref('mart_daily_mobility') }}
group by
    source_year,
    source_month,
    pickup_date_key
having count(*) > 1
