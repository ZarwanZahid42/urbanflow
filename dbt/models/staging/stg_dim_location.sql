select
    LOCATION_ID as location_id,
    BOROUGH as borough,
    ZONE as zone,
    SERVICE_ZONE as service_zone,
    BOROUGH_NORMALIZED as borough_normalized,
    ZONE_NORMALIZED as zone_normalized,
    SOURCE_FILE as source_file,
    INGESTED_AT_UTC as ingested_at_utc,
    BRONZE_RUN_ID as bronze_run_id,
    SILVER_RUN_ID as silver_run_id,
    SILVER_PROCESSED_AT_UTC as silver_processed_at_utc,
    GOLD_RUN_ID as gold_run_id,
    GOLD_PROCESSED_AT_UTC as gold_processed_at_utc
from {{ source('urbanflow_analytics', 'dim_location') }}
