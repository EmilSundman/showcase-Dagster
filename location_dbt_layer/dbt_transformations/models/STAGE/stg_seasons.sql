{{ config(
    dagster_freshness_policy={"maximum_lag_minutes": 1},
    dagster_auto_materialize_policy={"type":'eager'}
    
    ) 
}}

select 
    current_timestamp as load_datetime ,*
from {{ source('raw', 'raw_seasons') }}