import logging
import json
from pathlib import Path
import os

from dagster import (
    Definitions,
    load_assets_from_package_module,
    OpExecutionContext, 
    define_asset_job,
    BindResourcesToJobs
)

from dagster_duckdb_pandas import DuckDBPandasIOManager
from dagster_dbt.asset_decorator import dbt_assets
from dagster_dbt import dbt_cli_resource, load_assets_from_dbt_project, load_assets_from_dbt_manifest, dbt_assets,  DbtCliResource

from location_data_generators.assets import raw_data

MANIFEST_PATH = "/opt/dagster/app/dbt_transformations/target/manifest.json"
DBT_PROJECT_DIR = "/opt/dagster/app/dbt_transformations/."
DBT_PROFILES_DIR = "/opt/dagster/app/dbt_transformations/config/."
dbt_cli_args = ["--profiles-dir", f"{DBT_PROFILES_DIR}"]


raw_data_assets = load_assets_from_package_module(
    raw_data,
    group_name="CSV_loaders",
    # all of these assets live in the duckdb database, under the schema raw_data
    key_prefix=["raw_data", "duckster", "ducky"],
)
@dbt_assets(manifest=Path(MANIFEST_PATH))
def collection_of_dbt_assets(context: OpExecutionContext, dbt: DbtCliResource):
    dbt_task = dbt.cli(
        ["run", *dbt_cli_args], 
        manifest=MANIFEST_PATH, 
        context=context
        )
    yield from dbt_task.stream()

    # do something with run results or other artifacts
    try:
        run_results = dbt_task.get_artifact("run_results.json")
        context.log.info(f"Did {len(run_results['results'])} things.")
    except FileNotFoundError:
        context.log.info("No run results found.")

all_dbt_assets_job = define_asset_job(
    name="all_dbt_assets_job",
    selection=[collection_of_dbt_assets],
) 

resources = {
    # this io_manager allows us to load dbt models as pandas dataframes
    "io_manager": DuckDBPandasIOManager(
        database=f'md:duckster?motherduck_token={os.getenv("MOTHERDUCK_TOKEN")}'
        # database="duckster.duckdb"
        ),
    # this resource is used to execute dbt cli commands
    "dbt": DbtCliResource(
        project_dir=DBT_PROJECT_DIR,
        profiles_dir = DBT_PROFILES_DIR,
        profile="dbt_transformations",
        target="local"
        ), 
}

defs = Definitions(
    assets=[
            *raw_data_assets,
            collection_of_dbt_assets
            ],
    jobs=BindResourcesToJobs([all_dbt_assets_job]), 
    resources=resources,
)
