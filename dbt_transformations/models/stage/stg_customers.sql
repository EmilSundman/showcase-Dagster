{{
    config(
        dagster_auto_materialize_policy={"type":"lazy"}
    )
}}
select 
    * 
from {{source('raw', 'raw_customers')}}