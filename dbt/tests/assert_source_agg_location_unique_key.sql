select
    source_year,
    source_month,
    location_id
from {{ source('urbanflow_analytics', 'agg_location') }}
group by 1, 2, 3
having count(*) > 1
