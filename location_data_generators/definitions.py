import os

from dagster import (
    Definitions,
    load_assets_from_package_module,
)
from dagster_duckdb_pandas import DuckDBPandasIOManager


from location_data_generators.assets import raw_data


raw_data_assets = load_assets_from_package_module(
    raw_data,
    group_name="data_loaders",
    # all of these assets live in the duckdb database, under the schema raw_data
    key_prefix=["raw_data"],
)

resources = {
    # this io_manager allows us to load dbt models as pandas dataframes
    "io_manager": DuckDBPandasIOManager(database="duckster.duckdb"),
}

defs = Definitions(
    assets=[
            *raw_data_assets, 
            ],
    resources=resources,
)
