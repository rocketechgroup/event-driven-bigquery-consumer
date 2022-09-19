import os

from google.cloud import bigquery

PROJECT_ID = os.environ.get('PROJECT_ID')

# Construct a BigQuery client object.
client = bigquery.Client()
table_id = f'{PROJECT_ID}.event_driven_bigquery.us_states_by_ingestion_time'

job_config = bigquery.LoadJobConfig(
    schema=[
        bigquery.SchemaField("name", "STRING"),
        bigquery.SchemaField("post_abbr", "STRING")
    ],
    source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
)
uri = "gs://cloud-samples-data/bigquery/us-states/us-states.json"

load_job = client.load_table_from_uri(
    uri,
    table_id,
    location="US",  # Must match the destination dataset location.
    job_config=job_config,
)  # Make an API request.

load_job.result()  # Waits for the job to complete.

table_id_no_partition = f'{PROJECT_ID}.event_driven_bigquery.us_states_no_partition'

load_job = client.load_table_from_uri(
    uri,
    table_id_no_partition,
    location="US",  # Must match the destination dataset location.
    job_config=job_config,
)  # Make an API request.

load_job.result()  # Waits for the job to complete.

rows_to_insert = [
    {"name": "Alabama", "post_abbr": 'AL'},
    {"name": "Alaska", "post_abbr": 'AK'},
]

errors = client.insert_rows_json(table_id, rows_to_insert)  # Make an API request.
if not errors:
    print("New rows have been added.")
else:
    print("Encountered errors while inserting rows: {}".format(errors))
