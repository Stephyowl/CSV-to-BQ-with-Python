import datetime
import os
from pathlib import Path

import pandas as pd
from google.cloud import bigquery
from google.protobuf import descriptor_pb2
from google.cloud import bigquery_storage_v1
from google.cloud.bigquery_storage_v1 import writer

from google.cloud.bigquery_storage_v1.types import (
    WriteStream,
    AppendRowsRequest,
    ProtoSchema,
    ProtoRows,
    BatchCommitWriteStreamsRequest
)

import car_purchase_record_pb2

# Takes a CSV file and writes it to Google BigQuery using a Pending type stream.
# Pending was chosen because I wanted to commit rows in batches. 
def write_csv_to_bq(csv_file: str, project_id: str, dataset_id: str, table_id: str, chunk_size: int):
    write_client = bigquery_storage_v1.BigQueryWriteClient()
    parent = write_client.table_path(project_id, dataset_id, table_id)
    
    # In the event the stream crashes, we don't want to start from the beginning of the CSV file again.
    # So we split requests into chunks (easily done with pandas's read_csv method).
    # We track number of already sent chunks in a txt file named after the csv.
    Path("./progress").mkdir(parents=True, exist_ok=True)
    progress_file = f"./progress/{csv_file}.txt"
    if os.path.exists(progress_file):
        with open(progress_file, 'r') as file:
            chunks_read = int(file.read().strip())
    else:
        chunks_read = 0
        with open(progress_file, 'w') as file:
            file.write('0')
    
    print(f"chunk size: {chunk_size}, current chunks read: {chunks_read}")
    
    # enumerate() will add a counter to each element the iterable provides, allowing us to index each chunk
    # We can increment chunk_index until we've reached a chunk that hasn't been read yet.
    # because chunk_index starts at 0, the maximum value of chunk_index will be 1 less than the total number of chunks read
    # so we won't try to read a chunk that doesn't exist.
    chunk_iterator = pd.read_csv(csv_file, parse_dates=['Date'], chunksize=chunk_size)
    for chunk_index, chunk in enumerate(chunk_iterator):
        if chunk_index < chunks_read:
            continue  # skip read chunks
        
        print(f"Processing chunk {chunk_index+1}:")
        
        proto_rows = ProtoRows()
        
        for index, row in chunk.iterrows():
            print(f"Processing row {index}: {row.to_dict()}")
            row_data = create_row_data(row['Gender'], row['Age'], row['Price'], row['Date'].year, row['Date'].month, row['Date'].day)
            proto_rows.serialized_rows.append(row_data)     
        
        # after all rows in the chunk have been serialized and appended, we create a new write stream to handle the request
        # this effectively means chunk size = request size
        write_stream = write_client.create_write_stream(
            parent=parent, 
            write_stream= WriteStream(type_=WriteStream.Type.PENDING)
        )
        request = create_request(write_stream.name, proto_rows)
        requests = [request]
        
        def request_generator():
            for request in requests:
                yield request
        
        stream = write_client.append_rows(requests=request_generator())
        for response in stream:
                print(response)
        
        write_client.finalize_write_stream(name=write_stream.name)
        write_streams = [write_stream.name]
        response = batch_commit_write_streams(write_client, parent, write_streams)
        print(f"batch commit response: {response}")
        
        # Save the chunk progress file after the batch commit request
        # we do NOT want to consider this chunk processed if the stream failed to commit, so this is after the stream commits.
        chunks_read += 1
        with open(progress_file, 'w') as file:
            file.write(str(chunks_read))
        
        print(f"Finished processing chunk {chunk_index+1}.")
        # uncomment to process only one chunk per run
        break
    return 

def create_request(write_stream_name: str, proto_rows):
    # So that BigQuery knows how to parse the serialized_rows, generate a
    # protocol buffer representation of your message descriptor.
    proto_schema = ProtoSchema()
    proto_descriptor = descriptor_pb2.DescriptorProto()
    car_purchase_record_pb2.CarPurchaseRecord.DESCRIPTOR.CopyToProto(proto_descriptor)
    proto_schema.proto_descriptor = proto_descriptor

    request = AppendRowsRequest()
    request.write_stream = write_stream_name
    request.offset = 0 #required
    request.proto_rows = AppendRowsRequest.ProtoData(
        writer_schema=proto_schema,
        rows= proto_rows,
    )
    return request

def batch_commit_write_streams(write_client, parent: str, write_streams: list[str]):
    batch_commit_write_streams_request = BatchCommitWriteStreamsRequest(
        parent=parent, 
        write_streams= write_streams
    )
    response = write_client.batch_commit_write_streams(batch_commit_write_streams_request)
    return response

# Converts row data into data usable by the protocol buffer
def create_row_data(gender: str, age_num: int, car_price: float, year: int, month: int, day: int):
    row = car_purchase_record_pb2.CarPurchaseRecord()
    row.gender = gender
    row.age_num = age_num
    row.car_price = car_price
    row.date = generate_date(year, month, day)
    return row.SerializeToString()

# Helper function that converts a year, month and day into a value accepted by the DATE format.
def generate_date(year: int, month: int, day: int):
    date_value = datetime.date(year, month, day)
    epoch_value = datetime.date(1970, 1, 1)
    delta = date_value - epoch_value
    return delta.days

# Currently unused function
def create_csv_table(project_id: str, dataset_id: str, table_id: str):
    bigquery_client = bigquery.Client(project_id)
    schema = bigquery_client.schema_from_json("car_buyers_schema.json")
    full_table_id = f"{project_id}.{dataset_id}.{table_id}"
    table = bigquery.Table(full_table_id, schema=schema)
    table = bigquery_client.create_table(table, exists_ok=True)
    return full_table_id
    
if __name__ == "__main__":
    csv_file = "car_buyers_2024_slim.csv"
    project_id = "mythical-temple-451205-j6" 
    dataset_id = "testdataset1"
    table_id = "car_buyers_table"
    chunk_size = 5
    write_csv_to_bq(csv_file, project_id, dataset_id, table_id, chunk_size)

'''
 Next steps:
 - Automate detecting new csv files to upload and start the script when a new csv file is detected that hasn't been processed yet.
 Currently the progress for each csv is tracked by the generated .txt file but there should be a check for when a csv file is completely finished uploading.
 Also, the table the data gets sent to hardcoded in which is not ideal. Ideally a new table is created for each csv file which is why I made the create_csv_table function.
 The function wasn't used because while testing there was a significant delay between when the table is created and when the API detects that the table exists and allows writes.
 Functionality needs to be added to check if the table can be written to and only then call the write function.
 
 - This program only runs off of one stream. In order to batch process larger CSV files it's important to
 consider the scalability of this code. The choice to split the CSV file into chunks was made with future threading in mind.
 For each chunk, we can create a new thread with its own stream. This stream exclusively handles rows in the chunk given and
 upon successful insertion of all rows into BigQuery, it finalizes and commits the stream.
 For even faster data processing, we can use multiple hosts that each process one CSV file and run their own threads.
 But in order to accurately track which chunks fail to write into BQ, we can no longer rely on incrementing chunks_read because chunks won't process synchronously.
 
 - There is a 10MB limit per request. For extremely large CSV files it may be unfeasible to equate chunk size with request size.
 While chunk sizes can be made smaller, a batch commit request is made for each chunk. I believe batch committing requests is
 a fairly expensive operation because it requires finalizing the stream, preventing it from being re-used for other chunks and
 thus requiring a new write stream to be created per chunk. So smaller chunk sizes means more batch commit requests per csv file.
 
 In this case, using a stream manager with AppendRowsStream() and sending multiple requests per chunk is a possible solution.
 But this will require properly setting retry limits, timeouts and offsets so requests can be safely retried if they fail.
'''