
import random

from google.cloud import bigquery

def create_car_buyers_table(project_id: str, dataset_id: str):
    table_id = "car_buyers_table"
    
    bigquery_client = bigquery.Client(project_id)
    dataset = dataset_id
    schema = bigquery_client.schema_from_json("car_buyers_schema.json")
    full_table_id = f"{project_id}.{dataset}.{table_id}"
    table = bigquery.Table(full_table_id, schema=schema)
    table = bigquery_client.create_table(table, exists_ok=True)
    return full_table_id

if __name__ == "__main__":
    project_id = "mythical-temple-451205-j6"
    dataset_id = "testdataset1"
    full_table_id = create_car_buyers_table(project_id, dataset_id)
    print(full_table_id)