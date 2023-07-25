import os

from dagster import (
    Definitions,
    load_assets_from_package_module,
)

from location_data_generators.assets import data_generators


raw_data_assets = load_assets_from_package_module(
    data_generators,
    group_name="data_generators",
    # all of these assets live in the duckdb database, under the schema raw_data
    key_prefix=["raw_data"],
)


defs = Definitions(
    assets=[
            *raw_data_assets, 
            ],
)
