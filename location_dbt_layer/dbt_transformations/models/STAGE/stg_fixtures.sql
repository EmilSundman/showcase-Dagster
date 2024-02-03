{{ 
    config(
        materialized='incremental',
        unique_key='fixtureid',
        tags="daily_partition"
    )
}}

select 
    current_timestamp as load_datetime ,*
from {{ source('raw', 'raw_fixtures') }}

{% if is_incremental() %}

where DatePartition >= '{{var('partition_start_date')}}'::date
    and DatePartition < '{{var('partition_end_date')}}'::date
{% endif %}