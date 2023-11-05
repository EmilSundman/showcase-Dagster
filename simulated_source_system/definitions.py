import os
import duckdb
from duckdb.typing import * 
import faker 
import pandas as pd
from dagster import (
    Definitions,
    asset, 
    Output, 
    FilesystemIOManager,
    MetadataValue,
    ScheduleDefinition,
    define_asset_job,
    load_assets_from_package_module,
)

fake = faker.Faker()
def generate_person(seed):
    person = {
        'customer_id': fake.uuid4(),
        'name': fake.name(),
        'city': fake.city(),
        'state': fake.state(),
        'zip_code': fake.zipcode(),
        'country': fake.country(),
        'email': fake.email(),
        'job': fake.job(),
        'company': fake.company(),
        'ssn': fake.ssn(),
        'birthdate': fake.date_of_birth(),
        'phone_number': fake.phone_number()
    }
    return person

@asset(compute_kind="duckdb", group_name="SourceSystem")
def GenerateCustomers(context):
    # Connect to duckdb pointing to memory 
    con = duckdb.connect('ducky.duckdb')
    columns = {
        'customer_id': 'VARCHAR',
        'name': 'VARCHAR',
        'city': 'VARCHAR',
        'state': 'VARCHAR',
        'zip_code': 'VARCHAR',
        'country': 'VARCHAR',
        'email': 'VARCHAR',
        'job': 'VARCHAR',
        'company': 'VARCHAR',
        'ssn': 'VARCHAR',
        'birthdate': 'DATE',
        'phone_number': 'VARCHAR'
    }
    con.create_function(
    'generate_person',
    generate_person,
    [DOUBLE],
    duckdb.struct_type(columns)
    )
    res = con.sql("""
    SELECT person.* FROM (
        SELECT generate_person(random()) AS person
        FROM generate_series(1,5000)
    )
    """).df()
    
    # Return datafram res together with metadata
    return Output(
            res, 
            metadata={
                    "row_count": MetadataValue.int(len(res)), 
                    "columns": MetadataValue.json(columns), 
                    "preview": MetadataValue.md(res.head().to_markdown()),
                    "run_id": MetadataValue.url(f'http://localhost:3000/runs/{context.run_id}'),
                    },                    
            )

@asset(compute_kind="duckdb")
def My_New_asset():
    return 1

@asset(compute_kind="python", group_name="Mart")
def Dependent_asset(My_New_asset):
    return My_New_asset + 1

defs = Definitions(
    assets=[
            My_New_asset,
            Dependent_asset,
            GenerateCustomers
            ],
    # resources=resources,
    # schedules=[
    #     ScheduleDefinition(job=everything_job, cron_schedule="@weekly"),
    #     ScheduleDefinition(job=forecast_job, cron_schedule="@daily"),
    # ],
)
