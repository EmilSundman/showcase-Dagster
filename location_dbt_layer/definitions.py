from dagster import (
    Definitions,
)
from location_dbt_layer.dbt_assets import non_partitioned_dbt_assets, daily_partitioned_dbt_assets, DBT_PROJECT_DIR, DBT_PROFILES_DIR
from dagster_dbt import DbtCliResource
import os 

resources = {
    "LOCAL":{
    # this resource is used to execute dbt cli commands
    "dbt": DbtCliResource(
        project_dir=DBT_PROJECT_DIR,
        profiles_dir = DBT_PROFILES_DIR,
        profile="dbt_transformations",
        target='LOCAL'
        )
    },
    "DEV":{
    "dbt": DbtCliResource(
        project_dir=DBT_PROJECT_DIR,
        profiles_dir = DBT_PROFILES_DIR,
        profile="dbt_transformations",
        target='DEV'
        )
    },
    "PROD":{
    "dbt": DbtCliResource(
        project_dir=DBT_PROJECT_DIR,
        profiles_dir = DBT_PROFILES_DIR,
        profile="dbt_transformations",
        target='PROD'
        )
    }
}

defs = Definitions(
    assets=[
            non_partitioned_dbt_assets,
            daily_partitioned_dbt_assets
            ],
    resources=resources.get(os.getenv("ENVIRONMENT", "LOCAL")),
)