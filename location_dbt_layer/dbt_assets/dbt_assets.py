from pathlib import Path
import json
from dagster_dbt.asset_decorator import dbt_assets
from dagster_dbt import dbt_assets,  DbtCliResource, DagsterDbtTranslator, DagsterDbtTranslatorSettings
from dagster import (
    AssetExecutionContext,
    DailyPartitionsDefinition 
)

MANIFEST_PATH = "/opt/dagster/app/location_dbt_layer/dbt_transformations/target/manifest.json"
DBT_PROJECT_DIR = "/opt/dagster/app/location_dbt_layer/dbt_transformations/"
DBT_PROFILES_DIR = "/opt/dagster/app/location_dbt_layer/dbt_transformations/"
dbt_cli_args = ['--project-dir', f'{DBT_PROJECT_DIR}', '--profiles-dir', f'{DBT_PROFILES_DIR}']
DAILY_SEASON_PARTITIONS = DailyPartitionsDefinition(
    start_date="2023-07-01",
    end_date="2024-06-30",
)

dbt_translator = DagsterDbtTranslator(
    settings=DagsterDbtTranslatorSettings(
        enable_asset_checks=True # Change to True to enable asset checks
        )
    )

@dbt_assets(manifest=Path(MANIFEST_PATH), 
            select="fqn:*", 
            exclude="tag:daily_partition",
            dagster_dbt_translator=dbt_translator
            )
def non_partitioned_dbt_assets(
    context: AssetExecutionContext, 
    dbt: DbtCliResource
):
    
    dbt_task = dbt.cli(
        ["build", *dbt_cli_args], 
        context=context
        )
    
    yield from dbt_task.stream()
    
@dbt_assets(manifest=Path(MANIFEST_PATH), 
            select="tag:daily_partition", 
            dagster_dbt_translator=dbt_translator,
            partitions_def=DAILY_SEASON_PARTITIONS
            )
def daily_partitioned_dbt_assets(
    context: AssetExecutionContext, 
    dbt: DbtCliResource
):
    time_window = context.asset_partitions_time_window_for_output(
        list(context.selected_output_names)[0]
    )
    dbt_vars = {
        "partition_start_date": time_window.start.strftime("%Y-%m-%d"),
        "partition_end_date": time_window.end.strftime("%Y-%m-%d")
    }
    
    dbt_task = dbt.cli(
        ["build", 
         *dbt_cli_args,
        '--vars', 
        f'{json.dumps(dbt_vars)}'], 
        context=context
        )
    
    yield from dbt_task.stream()