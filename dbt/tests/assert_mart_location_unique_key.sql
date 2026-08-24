select
    source_year,
    source_month,
    location_id
from {{ ref('mart_location_mobility') }}
group by
    source_year,
    source_month,
    location_id
having count(*) > 1
