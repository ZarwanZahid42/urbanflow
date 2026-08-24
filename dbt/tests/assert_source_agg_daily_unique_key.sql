select
    source_year,
    source_month,
    pickup_date_key
from {{ source('urbanflow_analytics', 'agg_daily') }}
group by 1, 2, 3
having count(*) > 1
