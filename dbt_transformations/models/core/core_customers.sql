{{
    config(
        materialized='table', 
        dagster_auto_materialize_policy={"type":"lazy"}
    )
}}
select 
    * 
from {{ref("stg_customers")}}