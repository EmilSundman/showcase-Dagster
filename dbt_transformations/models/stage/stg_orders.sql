{{
    config(
        dagster_auto_materialize_policy={"type":"lazy"}
    )
}}

select 
    *, 
    current_timestamp as load_date
from {{source('raw', 'raw_orders_dataset')}}
-- Use the Dagster partition variables to filter rows on an incremental run
{% if is_incremental() %} 
where order_approved_at >= '{{ var('start_ts') }}' :: datetime 
    and order_approved_at < '{{ var('end_ts') }}' :: datetime
{% endif %}
