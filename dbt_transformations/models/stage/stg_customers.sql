{{
    config(
        materialized='view', 

        dagster_freshness_policy={"maximum_lag_minutes": 5},
        dagster_auto_materialize_policy={"type":"eager"}
    )
}}
select 
    * 
from {{source('raw', 'raw_customers')}}