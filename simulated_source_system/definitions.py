import os 
import pandas as pd
from dagster import (
    Definitions,
    load_assets_from_package_module,
)
from dagster_duckdb_pandas import DuckDBPandasIOManager
from simulated_source_system.assets import source_system

source_system = load_assets_from_package_module(
    source_system,
    group_name="source_system",
)

resources = {
    # this io_manager allows us to load dbt models as pandas dataframes
    "duckdb_IOManager": DuckDBPandasIOManager(
        database=f'{os.getenv("MOTHERDUCK_PATH")}', 
        schema="raw", 
        ),
}


defs = Definitions(
    assets=[
            *source_system,
            ],
    resources=resources,
    # schedules=[
    #     ScheduleDefinition(job=everything_job, cron_schedule="@weekly"),
    #     ScheduleDefinition(job=forecast_job, cron_schedule="@daily"),
    # ],
)