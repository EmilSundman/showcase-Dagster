{{
    config(
        materialized='table',
        on_schema_change='append_new_columns',
        dagster_auto_materialize_policy={"type":"lazy"}
    )
}}

select 
    *, 
    current_timestamp as load_date
from {{ref("stg_orders")}}
