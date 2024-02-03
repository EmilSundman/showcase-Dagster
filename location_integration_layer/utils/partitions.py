from dagster import DailyPartitionsDefinition,DynamicPartitionsDefinition, StaticPartitionsDefinition, MultiPartitionsDefinition

INCLUDED_LEAGUES = [
    39 # Premier League
]


DAILY_SEASON_PARTITIONS = DailyPartitionsDefinition(
    start_date="2023-07-01",
    end_date="2024-06-30",
)

FIXTURES_PARTITIONS = DynamicPartitionsDefinition(
    name="fixtures_partitions",
)

DAILY_FIXTURES_PARTITIONS = MultiPartitionsDefinition(
    {   
    'Date': DAILY_SEASON_PARTITIONS,  
    'FixtureId': FIXTURES_PARTITIONS
    }
)