{{ config(
    dagster_freshness_policy={"maximum_lag_minutes": 60, "cron_schedule": "0 6 * * *"},
    dagster_auto_materialize_policy={"type":'lazy'}
    ) 
}}


select 
    current_timestamp as load_datetime ,*
from {{ source('raw', 'raw_fixture_player_stats') }}