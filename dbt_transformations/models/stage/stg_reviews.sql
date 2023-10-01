{{
    config(
        materialized='incremental',
        on_schema_change='append_new_columns',
        unique_key='review_id',
        tags=["partition_daily"], 
        dagster_freshness_policy={"maximum_lag_minutes": 30},
        dagster_auto_materialize_policy={"type":"eager"}
    )
}}

with source as (
      select * from {{ source('raw', 'raw_order_reviews_dataset') }}
),
renamed as (
    select
        *,
        current_timestamp as load_date
    from source
)
select * from renamed
-- Use the Dagster partition variables to filter rows on an incremental run
{% if is_incremental() %} 
where review_creation_date >= '{{ var('start_ts') }}' :: datetime 
    and review_creation_date < '{{ var('end_ts') }}' :: datetime
{% endif %}