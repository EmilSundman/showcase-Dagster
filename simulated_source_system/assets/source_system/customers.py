import faker 
import duckdb
import pandas as pd
from duckdb.typing import *  
import random
from dagster import (asset, Output, MetadataValue, FreshnessPolicy, AutoMaterializePolicy)


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

@asset(compute_kind="duckdb", 
        freshness_policy=FreshnessPolicy(maximum_lag_minutes=2),
        auto_materialize_policy=AutoMaterializePolicy.eager()
        )
def GenerateCustomers(context) -> Output:
    """
    Generate a random set of customer data.

    Args:
        context (dagster.core.execution.contexts.system.SystemExecutionContext): The execution context.

    Returns:
        Output: The generated customer data as a pandas DataFrame, along with metadata.

    Raises:
        None
    """

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

    random_number = random.randint(9000, 20000)
    res = con.sql(f"""
    SELECT person.* FROM (
        SELECT generate_person(random()) AS person
        FROM generate_series(1,{random_number})
    )
    """).df()
    
    # Return dataframe res together with metadata
    return Output(
        res, 
        metadata={
            "row_count": MetadataValue.int(len(res)), 
            "columns": MetadataValue.json(columns), 
            "preview": MetadataValue.md(res.head().to_markdown()),
            "run_id": MetadataValue.url(f'http://localhost:3000/runs/{context.run_id}'),
        },                    
    )
