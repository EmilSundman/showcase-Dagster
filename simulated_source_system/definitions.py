import os

from dagster import (
    Definitions,
    asset, 
    FilesystemIOManager,
    ScheduleDefinition,
    define_asset_job,
    load_assets_from_package_module,
)
@asset(compute_kind="duckdb")
def My_New_asset():
    return 1

@asset(compute_kind="python", group_name="Mart")
def Dependent_asset(My_New_asset):
    return My_New_asset + 1

defs = Definitions(
    assets=[
            My_New_asset,
            Dependent_asset,
            ],
    # resources=resources,
    # schedules=[
    #     ScheduleDefinition(job=everything_job, cron_schedule="@weekly"),
    #     ScheduleDefinition(job=forecast_job, cron_schedule="@daily"),
    # ],
)
