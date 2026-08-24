select
    source_year,
    source_month,
    pickup_date_key,
    hour
from {{ source('urbanflow_analytics', 'agg_hourly') }}
group by 1, 2, 3, 4
having count(*) > 1
