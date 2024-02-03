from typing import Any
from dagster import IOManager, io_manager, Field, String, MetadataValue, get_dagster_logger
from dagster._core.execution.context.input import InputContext
from dagster._core.execution.context.output import OutputContext
from google.cloud import storage
import pandas as pd
import json

logger = get_dagster_logger()

class GCPStorageIOManager(IOManager):
    def __init__(self, bucket_name, *args, **kwargs):
        self.client = storage.Client()
        self.bucket = self.client.get_bucket(bucket_name)
        super().__init__(*args, **kwargs)
    
    def handle_output(self, context: OutputContext, obj: Any) -> None:
        
        # Determine name of destination blob
        blobster = context.get_asset_identifier()
        blob_path = f'{"/".join(blobster)}/{"-".join(blobster)}'
        
        # Determine the filename and format based on the type of the object
        if isinstance(obj, pd.DataFrame):
            file_path = f"{blob_path}.csv"
            # Convert DataFrame to CSV and upload
            blob = self.bucket.blob(file_path)
            blob.upload_from_string(obj.to_csv(index=False), content_type='text/csv')
            context.add_output_metadata(
                {
                    "file_type": "csv",
                    "file_path": file_path
                }
            )
            
        elif isinstance(obj, dict):
            file_path = f"{blob_path}.json"
            # Convert dict to JSON and upload
            blob = self.bucket.blob(file_path)
            blob.upload_from_string(json.dumps(obj), content_type='application/json')
            context.add_output_metadata(
                {
                    "file_type": "json",
                    "file_path": file_path
                }
            )
                        
        else:
            # Handle other object types or raise an error
            raise ValueError(f"Unsupported type for output: {type(obj)}")
    
    def load_input(self, context: InputContext) -> Any:
        
        # Get name of source blob to fetch
        blobster = context.get_asset_identifier()
        blob_path = f'{"/".join(blobster)}/{"-".join(blobster)}'
        
        # ^ Note, IO Manager only handles JSON files for now... 
        file_path = f"{blob_path}.json"
        blob = self.bucket.blob(file_path)
        return json.loads(blob.download_as_string())
        

@io_manager(config_schema={"bucket": Field(String, is_required=True)})
def gcp_storage_io_manager(init_context):
    # Extract the bucket from the config
    bucket_name = init_context.resource_config["bucket"]
    return GCPStorageIOManager(bucket_name=bucket_name)