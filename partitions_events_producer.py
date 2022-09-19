import os
import json
import pendulum

from google.cloud import bigquery

PROJECT_ID = os.environ.get('PROJECT_ID')
client = bigquery.Client()

checkpoint_partition = 'checkpoints/partition_checkpoint.json'
now = pendulum.now("UTC")

try:
    with open(checkpoint_partition, 'r') as fp:
        checkpoint_partition_info = json.loads(fp.read())
except FileNotFoundError as e:
    checkpoint_partition_info = {
        'checkpoint_timestamp': now.to_iso8601_string()
    }

partitioned_events_query = f"""
    SELECT
      table_catalog as project_id,	
      table_schema as dataset_id,	
      table_name as table_id,	
      'INFORMATION_SCHEMA.PARTITIONS' as event_type, 
      min(last_modified_time) as min_event_timestamp,
      max(last_modified_time) as max_event_timestamp,
      '{checkpoint_partition_info['checkpoint_timestamp']}' as from_timestamp, 
      '{now.to_iso8601_string()}' as to_timestamp
    FROM
     `event_driven_bigquery.INFORMATION_SCHEMA.PARTITIONS`
    WHERE last_modified_time > '{checkpoint_partition_info['checkpoint_timestamp']}'
    GROUP BY table_catalog,	table_schema, table_name, event_type
"""
print(partitioned_events_query)
partitioned_events = client.query(partitioned_events_query)

results = partitioned_events.result()
with open('output/events.jsonl', 'a') as fp:
    for row in results:
        row_dict = dict(row.items())
        print(row_dict)
        fp.write(json.dumps(row_dict, default=str) + '\n')

# update checkpoint
checkpoint_partition_info = {
    'checkpoint_timestamp': now.to_iso8601_string()
}
with open(checkpoint_partition, 'w') as fp:
    fp.write(json.dumps(checkpoint_partition_info))
