
import random

from google.cloud import bigquery

def create_car_buyers_table(project_id: str, dataset_id: str):
    bigquery_client = bigquery.Client(project_id)
    dataset = dataset_id
    schema = bigquery_client.schema_from_json("car_buyers_schema.json")
    table_id = "car_buyers_table"
    full_table_id = f"{project_id}.{dataset}.{table_id}"
    table = bigquery.Table(full_table_id, schema=schema)
    table = bigquery_client.create_table(table, exists_ok=True)
    return full_table_id

if __name__ == "__main__":
    full_table_id = create_car_buyers_table("mythical-temple-451205-j6", "testdataset1")
    print(full_table_id)