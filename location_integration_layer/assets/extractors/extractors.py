from dagster import (
    asset,
    get_dagster_logger,
    Output, 
    MetadataValue,
    AutoMaterializePolicy,
    FreshnessPolicy,
    AddDynamicPartitionsRequest,
    AssetExecutionContext,
    multi_asset,
    AssetOut,
    DailyPartitionsDefinition
    
)
from typing import * 
from google.cloud import storage
from utils.partitions import DAILY_SEASON_PARTITIONS, INCLUDED_LEAGUES, FIXTURES_PARTITIONS, DAILY_FIXTURES_PARTITIONS

logger = get_dagster_logger()

# ENDPOINTS
ENDPOINT_LEAGUES = "leagues"
ENDPOINT_FIXTURES = "fixtures"
ENDPOINT_FIXTURE_EVENTS = "fixtures/events"
ENDPOINT_FIXTURE_PLAYER_STATS = "fixtures/players"
ENDPOINT_FIXTURE_STATS = "fixtures/statistics"
SELECTED_FIXTURE = "1035375"  # Need a solution for handling many fixtures.

# PARTITIONS 
DAILY_SEASON_PARTITIONS = DailyPartitionsDefinition(
    start_date="2023-07-01",
    end_date="2024-06-30",
)

@asset(
    required_resource_keys={"api_football_client"},
    io_manager_key="gcp_storage_io_manager",
    config_schema={
        "api_parameters": {},
        },
    compute_kind="python",
    auto_materialize_policy=AutoMaterializePolicy.lazy()
)
def extract_leagues(context) -> Output[dict]:
    """
    Sends a get request with the specifed parameters.
    Returns a JSON-object from the response.
    """
    endpoint = ENDPOINT_LEAGUES
    params = context.op_config["api_parameters"]
    api_response = context.resources.api_football_client.fetch_data(endpoint, params)
    return Output(
        api_response, 
        metadata={
            # Get length of api_response
            "Response Size": MetadataValue.int(len(api_response["response"])),
            # Get the json structure of the first element in api_response
            "Response Example": MetadataValue.json(api_response["response"][0]),
            "Request Parameters": MetadataValue.json(params),
        }
    )

@asset(
    partitions_def=DAILY_SEASON_PARTITIONS,
    required_resource_keys={"api_football_client"},
    io_manager_key="gcp_storage_io_manager",
    compute_kind="python", 
    auto_materialize_policy=AutoMaterializePolicy.lazy()
)
def extract_fixtures(context: AssetExecutionContext) -> Output[dict]:
    """
    ## `extract_fixtures` function

    This function is a Dagster asset that extracts football fixture data from an API.

    ### Parameters:
    - `context` (`AssetExecutionContext`): The execution context of the asset.

    ### Returns:
    - `Output[dict]`: The output is a dictionary containing the API response and some metadata. The metadata includes the number of fixtures, an example response, and the request parameters.

    ### Behavior:
    The function fetches data from the `ENDPOINT_FIXTURES` endpoint for a specific date, which is determined by the asset's partition key. It then iterates over the response to generate dynamic partitions for each fixture ID if the league ID is in `INCLUDED_LEAGUES`. The function logs the dynamic partitions and returns the API response along with some metadata.

    ### Example:
    ```python
    context = ...  # An instance of AssetExecutionContext
    output = extract_fixtures(context)
    print(output.value)  # Prints the API response
    print(output.metadata)  # Prints the metadata
    ```
    """
    endpoint = ENDPOINT_FIXTURES
    partition_date = context.asset_partition_key_for_output()
    params = {"date": partition_date}
    api_response = context.resources.api_football_client.fetch_data(endpoint, params)
    
    # Generate dynamic partitions and create a request for each fixture_id if the league_id is in INCLUDED_LEAGUES
    dynamic_partitions = []
    for fixture in api_response["response"]:
        if fixture["league"]["id"] in INCLUDED_LEAGUES:
            dynamic_partitions.append(str(fixture["fixture"]["id"]))
    logger.info(f"Dynamic partitions: {dynamic_partitions}")
    
    return Output(
        api_response, 
        metadata={
            # Get length of api_response
            "Number of fixtures": MetadataValue.int(len(api_response["response"])),
            # Get the json structure of the first element in api_response
            "Response Example": MetadataValue.json(api_response["response"][0]),
            "Request Parameters": MetadataValue.json(params),
        }
    )

@asset(
    required_resource_keys={"api_football_client"},
    io_manager_key="gcp_storage_io_manager",
    compute_kind='python'
)
def extract_fixture_events(context) -> Output[dict]:
    # <!> Need to be partitioned by fixtures our something like that. Maybe two dimensions with league and date partition. Then a loop through fixtures before concating into one dict.
    """
    Sends a get request with the specifed parameters.
    Returns a JSON-object from the response.
    """
    endpoint = ENDPOINT_FIXTURE_EVENTS
    # Select Arsenal - Palace (2024-01-20)
    params = {"fixture": SELECTED_FIXTURE}
    result = context.resources.api_football_client.fetch_data(endpoint, params)
    return Output(result, metadata={"Response Size": len(result["response"])})

@asset(
    required_resource_keys={"api_football_client"},
    io_manager_key="gcp_storage_io_manager",
    compute_kind='python'
)
def extract_fixture_stats(context) -> Output[dict]:
    # ? Should be partitioned by fixtures our something like that. Maybe two dimensions with league and date partition. Then a loop through fixtures before concating into one dict.
    """
    Sends a get request with the specifed parameters.
    Returns a JSON-object from the response.
    """
    endpoint = ENDPOINT_FIXTURE_STATS
    # Select Arsenal - Palace (2024-01-20)
    params = {"fixture": SELECTED_FIXTURE}
    result = context.resources.api_football_client.fetch_data(endpoint, params)
    return Output(result, metadata={"Response Size": len(result["response"])})

@asset(
    required_resource_keys={"api_football_client"},
    io_manager_key="gcp_storage_io_manager",
    compute_kind='python',
    auto_materialize_policy=AutoMaterializePolicy.lazy()
)
def extract_fixture_player_stats(context) -> Output[dict]:
    # <!> Need to be partitioned by fixtures our something like that. Maybe two dimensions with league and date partition. Then a loop through fixtures before concating into one dict.
    """
    Sends a get request with the specifed parameters.
    Returns a JSON-object from the response.
    """
    endpoint = ENDPOINT_FIXTURE_PLAYER_STATS
    # Select Arsenal - Palace (2024-01-20)
    params = {"fixture": SELECTED_FIXTURE}
    result = context.resources.api_football_client.fetch_data(endpoint, params)
    return Output(result, metadata={"Response Size": len(result["response"])})