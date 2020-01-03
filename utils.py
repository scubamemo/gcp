import json
from google.cloud import bigquery
from google.cloud import storage

client = bigquery.Client()
storage_client = storage.Client()


def get_table_full_name(dataset, table):
    return dataset + '.' + table


def load_schema_data(bucket, file):
    schema_blob = storage_client.get_bucket(bucket).blob(file)
    table_schema_data = json.loads(schema_blob.download_as_string())

    schema_data = []
    for p in table_schema_data:
        schema_data.append(bigquery.SchemaField(p['name'], p['type']))

    return schema_data
