select
    location_aggregate.source_year,
    location_aggregate.source_month,
    location_aggregate.location_id,
    location_dimension.borough,
    location_dimension.zone,
    location_dimension.service_zone,
    location_dimension.borough_normalized,
    location_dimension.zone_normalized,
    location_aggregate.pickup_trip_count,
    location_aggregate.dropoff_trip_count,
    location_aggregate.total_revenue,
    location_aggregate.average_trip_distance,
    location_aggregate.average_total_amount,
    location_aggregate.total_distance,
    location_aggregate.gold_run_id,
    location_aggregate.gold_processed_at_utc
from {{ ref('stg_agg_location') }} as location_aggregate
inner join {{ ref('stg_dim_location') }} as location_dimension
    on location_aggregate.location_id = location_dimension.location_id
