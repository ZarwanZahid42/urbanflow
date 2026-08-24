with
    source_count as (
        select count(*) as row_count
        from {{ ref('stg_fact_trips') }}
    ),

    enriched_count as (
        select count(*) as row_count
        from {{ ref('int_trip_enriched') }}
    )

select
    source_count.row_count as source_row_count,
    enriched_count.row_count as enriched_row_count
from source_count
cross join enriched_count
where source_count.row_count <> enriched_count.row_count
