import logging
import json
from pathlib import Path
import os

from dagster import (
    Definitions,
    load_assets_from_package_module,
    AssetExecutionContext, 
    define_asset_job,
    DailyPartitionsDefinition,
    BindResourcesToJobs
)

from dagster_duckdb_pandas import DuckDBPandasIOManager
from dagster_dbt.asset_decorator import dbt_assets
from dagster_dbt import dbt_cli_resource, load_assets_from_dbt_project, load_assets_from_dbt_manifest, dbt_assets,  DbtCliResource

from location_data_generators.assets import raw_data
from location_data_generators.assets.raw_data.load_data import asset_not_empty

MANIFEST_PATH = "/opt/dagster/app/dbt_transformations/target/manifest.json"
DBT_PROJECT_DIR = "/opt/dagster/app/dbt_transformations/"
DBT_PROFILES_DIR = "/opt/dagster/app/dbt_transformations/"
dbt_cli_args = ['--project-dir', f'{DBT_PROJECT_DIR}', '--profiles-dir', f'{DBT_PROFILES_DIR}']


raw_data_assets = load_assets_from_package_module(
    raw_data,
    group_name="CSV_loaders",
    # all of these assets live in the duckdb database, under the schema raw_data
    key_prefix=["raw"]
)

@dbt_assets(manifest=Path(MANIFEST_PATH), 
            select="tag:dagster_asset", 
            exclude="tag:partition_daily"
            )
def base_models(context: AssetExecutionContext, dbt: DbtCliResource):
    
    dbt_task = dbt.cli(
        ["build", *dbt_cli_args], 
        context=context
        )
    yield from dbt_task.stream()
    
@dbt_assets(manifest=Path(MANIFEST_PATH), 
            select="tag:partition_daily", 
            partitions_def=DailyPartitionsDefinition(start_date="2016-01-01")
            )
def daily_partitioned_models(context: AssetExecutionContext, dbt: DbtCliResource):
    time_window = context.asset_partitions_time_window_for_output(
        list(context.selected_output_names)[0]
    )
    dbt_vars = {
        "start_ts": time_window.start.isoformat(),
        "end_ts": time_window.end.isoformat()
    }
    
    dbt_task = dbt.cli(
        ["build", 
        *dbt_cli_args, 
        '--vars', 
        f'"{json.dumps(dbt_vars)}"'
        ], 
        context=context
        )
    yield from dbt_task.stream()

    # do something with run results or other artifacts
    # try:
    #     run_results = dbt_task.get_artifact("run_results.json")
    #     context.log.info(f"Did {len(run_results['results'])} things.")
    # except FileNotFoundError:
    #     context.log.info("No run results found.")

all_dbt_assets_job = define_asset_job(
    name="all_dbt_assets_job",
    selection=[base_models],
    ) 

resources = {
    # this io_manager allows us to load dbt models as pandas dataframes
    "io_manager": DuckDBPandasIOManager(
        database=f'{os.getenv("MOTHERDUCK_PATH")}',
        # database=f'duckster',
        ),
    # this resource is used to execute dbt cli commands
    "dbt": DbtCliResource(
        project_dir=DBT_PROJECT_DIR,
        profiles_dir = DBT_PROFILES_DIR,
        profile="dbt_transformations",
        target="motherduck"
        ), 
}

defs = Definitions(
    assets=[
            *raw_data_assets,
            base_models, 
            daily_partitioned_models
            ],
    jobs=BindResourcesToJobs([all_dbt_assets_job]), 
    resources=resources,
)
