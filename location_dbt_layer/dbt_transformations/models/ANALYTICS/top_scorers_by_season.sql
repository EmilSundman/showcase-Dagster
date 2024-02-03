{{ config(
    dagster_freshness_policy={"maximum_lag_minutes": 5},
    dagster_auto_materialize_policy={"type":'lazy'}
    ) 
}}

select 
    current_timestamp as load_datetime,
    fix.season,
    league.name as leaguename,
    p_stats.playername,
    sum(coalesce(p_stats."goals total"::int, 
    0)) as goals
from {{ref("stg_fixture_player_stats")}} p_stats
left join {{ref("stg_fixtures")}} fix 
    on p_stats.fixtureid = fix.fixtureid
left join {{ref("stg_leagues")}} league 
    on fix.leagueid = league.leagueid
group by all 
having goals > 0
order by goals desc