from dagster import (
    RunRequest,
    SensorResult,
    sensor,
    SensorEvaluationContext,
    AssetSelection,
    get_dagster_logger,
    AddDynamicPartitionsRequest
)

logger = get_dagster_logger()

@sensor(asset_selection=AssetSelection.keys('extract_fixtures'))
def add_dynamic_partition_to_fixtures_partitions(context: SensorEvaluationContext):
    """
    Generates a partition for each fixture_id in the api_response.
    ! Not working yet
    """
    asset_keys = context.instance.get_asset_keys() 
    logger.info(f'Asset keys: {asset_keys}')
    partitions = context.instance.get_materialized_partitions(asset_key=asset_keys[0])
    logger.info(f'Partitions: {partitions}')
    SensorResult(
        dynamic_partitions_requests=[
            AddDynamicPartitionsRequest(
                partitions_def_name="fixtures_partitions",
                partition_keys=["868209"]
            )
        ]
    )