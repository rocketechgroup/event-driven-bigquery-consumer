# Event Driven BigQuery Consumer
Track BigQuery table updates and turn them into events

## Concept
Typically, consuming data from BigQuery is via scheduling there isn't any concept of event driven processing. 
Having said that, BigQuery offers two very interesting INFORMATION_SCHEMA tables which we can utilise and turn these into events for downstream consumers. 

In this example, we are making the use of 
- Streaming table https://cloud.google.com/bigquery/docs/information-schema-streaming
- Partition table https://cloud.google.com/bigquery/docs/information-schema-partitions

And designing a system to track changing in a centralised process and turn them into events which allows downstream consumers to be events driven

## Examples
### Data Producer
Use this script to produce some sample data that triggers the updates in INFORMATION_SCHEMA
Script: [data_producer.py](data_producer.py)

### Partitions Events Producer
Use this script to produce some sample data that triggers the updates in INFORMATION_SCHEMA
Script: [partitions_events_producer.py](partitions_events_producer.py)

### Stream Events Producer
Use this script to produce some sample data that triggers the updates in INFORMATION_SCHEMA
Script: [stream_events_producer.py](stream_events_producer.py)


## Additional Considerations
- Use hash comparison in the checkpointing process to track changes at table level hence reduce duplicates
- Use better storage types to track checkpoints, such as [Cloud Datastore](https://cloud.google.com/datastore)
- Send events to [Cloud PubSub](https://cloud.google.com/pubsub) so that downstream consumers can be added