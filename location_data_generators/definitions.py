import os

from dagster import (
    Definitions,
    load_assets_from_package_module,
    file_relative_path
)
from dagster_duckdb_pandas import DuckDBPandasIOManager
from location_data_generators.assets import raw_data
from dagster_dbt import dbt_cli_resource, load_assets_from_dbt_project


DBT_PROJECT_DIR = file_relative_path(__file__, "dbt_transformations")
DBT_PROFILES_DIR = file_relative_path(__file__, "dbt_transformations/config")

raw_data_assets = load_assets_from_package_module(
    raw_data,
    group_name="data_loaders",
    # all of these assets live in the duckdb database, under the schema raw_data
    key_prefix=["raw_data", "ducky"],
)



dbt_assets = load_assets_from_dbt_project(
    DBT_PROJECT_DIR,
    DBT_PROFILES_DIR,
    # prefix the output assets based on the database they live in plus the name of the schema
    key_prefix=["duckdb", "ducky"],
    # prefix the source assets based on just the database
    # (dagster populates the source schema information automatically)
    source_key_prefix=["ducky"],
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
