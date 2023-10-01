{{
    config(
        materialized='table',
        on_schema_change='append_new_columns',
        dagster_freshness_policy={"maximum_lag_minutes": 5},
        dagster_auto_materialize_policy={"type":"eager"}
    )
}}

with top_customers as (
select 
    cust.customer_id,
    count(distinct orders.order_id) as order_count
from {{ref("core_customers")}} cust 
inner join {{ref("core_orders")}} orders
    on cust.customer_id = orders.customer_id
group by all 
) 
select 
    *, 
    current_timestamp as load_date 
from top_customers
