import os 
from dagster import (
    Definitions,
    asset,
    load_assets_from_package_module,
    fs_io_manager,
)
from dagster_duckdb_pandas import DuckDBPandasIOManager
from assets import extractors, loaders
from resources import api_football_client
from resources import gcp_storage_io_manager
from sensors import add_dynamic_partition_to_fixtures_partitions

extractors = load_assets_from_package_module(
    package_module=extractors,
    group_name="EXTRACTORS"
)
loaders = load_assets_from_package_module(
    package_module=loaders,
    group_name="LOADERS",
    key_prefix="raw" # Will dictate the schema of the output
)

@asset(group_name='EXAMPLE')
def HelloWorld():
    return "Hello World!"

@asset(group_name='EXAMPLE')
def LettersInHelloWorld(HelloWorld):
    """
    # This is a markdown string
    This asset takes a string and returns the number of letters in the string.
    ## Args:
    HelloWorld (str): The string to count the letters of.
    ## Returns:
    int: The number of letters in the string.
    
    """
    return len(HelloWorld)
    
resources = {
    "LOCAL": {
        "api_pickle_json": fs_io_manager,
        "gcp_storage_io_manager": fs_io_manager,
        "api_football_client": api_football_client,
        "duckdb_io_manager": DuckDBPandasIOManager(database=f'{os.getenv("DUCKDB_DB")}')
        },
    "DEV": {
        "api_pickle_json": fs_io_manager,
        "gcp_storage_io_manager": gcp_storage_io_manager.configured({"bucket":"dev-football_data"}),
        "api_football_client": api_football_client,
        "duckdb_io_manager": DuckDBPandasIOManager(database=f'{os.getenv("MOTHERDUCK_PATH")}')
        },
    "PROD": {
        "api_pickle_json": fs_io_manager,
        "gcp_storage_io_manager": gcp_storage_io_manager.configured({"bucket":"prod-football_data"}),
        "api_football_client": api_football_client,
        "duckdb_io_manager": DuckDBPandasIOManager(database=f'{os.getenv("MOTHERDUCK_PATH")}'),
        }
    }

defs = Definitions(
    assets=[
        # HelloWorld,
        # LettersInHelloWorld,
        *extractors,
        *loaders
    ], 
    resources=resources.get(os.getenv("ENVIRONMENT", "LOCAL"))
    )