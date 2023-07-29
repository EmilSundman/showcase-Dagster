{# {{
    config(
            dagster_auto_materialize_policy={"type":"lazy"}, 
            dagster_freshness_policy={"cron_schedule": "0 9 * * *", "maximum_lag_minutes": (2)*60}

    )
}} #}

select * from {{source('raw', 'raw_customers')}}