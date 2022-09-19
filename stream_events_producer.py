import os
import json
import pendulum

from google.cloud import bigquery

PROJECT_ID = os.environ.get('PROJECT_ID')
client = bigquery.Client()

checkpoint_partition = 'checkpoints/stream_checkpoint.json'
now = pendulum.now("UTC")

try:
    with open(checkpoint_partition, 'r') as fp:
        checkpoint_partition_info = json.loads(fp.read())
except FileNotFoundError as e:
    checkpoint_partition_info = {
        'checkpoint_timestamp': now.to_iso8601_string()
    }

checkpoint_minus_10_mins = pendulum.parse(checkpoint_partition_info['checkpoint_timestamp']) \
    .subtract(minutes=10).to_iso8601_string()

stream_events_query = f"""
    SELECT
       project_id,	
       dataset_id,	
       table_id,	
       'INFORMATION_SCHEMA.STREAMING_TIMELINE_BY_PROJECT' as event_type, 
       min(start_timestamp) as min_event_timestamp,
       max(start_timestamp) as max_event_timestamp,
       '{checkpoint_minus_10_mins}' as from_timestamp, 
       '{now.to_iso8601_string()}' as to_timestamp
    FROM
      `region-us`.INFORMATION_SCHEMA.STREAMING_TIMELINE_BY_PROJECT
    WHERE
      error_code IS NULL
      AND start_timestamp > '{checkpoint_minus_10_mins}'
    
    GROUP BY project_id, dataset_id, table_id, event_type
"""

print(stream_events_query)
stream_events = client.query(stream_events_query)

results = stream_events.result()
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
