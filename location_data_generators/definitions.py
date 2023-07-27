import logging
import json
from pathlib import Path
import os

from dagster import (
    Definitions,
    load_assets_from_package_module,
    OpExecutionContext
)

from dagster_duckdb_pandas import DuckDBPandasIOManager
from dagster_dbt.asset_decorator import dbt_assets
from dagster_dbt import dbt_cli_resource, load_assets_from_dbt_project, load_assets_from_dbt_manifest

from location_data_generators.assets import raw_data

MANIFEST_PATH = "/opt/dagster/app/dbt_transformations/target/manifest.json"
DBT_PROJECT_DIR = "/opt/dagster/app/dbt_transformations/."
DBT_PROFILES_DIR = "/opt/dagster/app/dbt_transformations/config/."

raw_data_assets = load_assets_from_package_module(
    raw_data,
    group_name="CSV_loaders",
    # all of these assets live in the duckdb database, under the schema raw_data
    key_prefix=["raw_data", "duckster"],
)


dbt_assets=load_assets_from_dbt_manifest(
    Path(MANIFEST_PATH), 
    key_prefix=["duckdb", "duckster"],
    # prefix the source assets based on just the database
    # (dagster populates the source schema information automatically)
    source_key_prefix=["duckster"],
    )

resources = {
    # this io_manager allows us to load dbt models as pandas dataframes
    "io_manager": DuckDBPandasIOManager(database="duckster.duckdb"),
        # this resource is used to execute dbt cli commands
    "dbt": dbt_cli_resource.configured(
        {"project_dir": DBT_PROJECT_DIR, "profiles_dir": DBT_PROFILES_DIR}
    )
}

defs = Definitions(
    assets=[
            *raw_data_assets,
            *dbt_assets 
            ],
    resources=resources,
)
