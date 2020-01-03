import json
from google.cloud import bigquery
from google.cloud.bigquery.external_config import HivePartitioningOptions
from google.cloud import storage
import utils

client = bigquery.Client()
storage_client = storage.Client()

ds_id = 'new_york_taxi_trips'
destination_table_name = 'trips_data'
bucket_name = 'sample_data'
schema_file_name = 'trips_schema.json'


def validate_data(dataset_ref, table_name):
    destination_table = client.get_table(dataset_ref.table(table_name))
    print("Loaded {} rows.".format(destination_table.num_rows))


def run_job(dataset_ref, uri, job_config):
    load_job = client.load_table_from_uri(
        uri,
        dataset_ref.table(destination_table_name),
        location="US",
        job_config=job_config,
    )  # API request
    print("Starting job {}".format(load_job.job_id))

    load_job.result()  # Wait for table load to complete.


def configure_job(bucket, schema_file):
    job_config = bigquery.LoadJobConfig()
    # Load Table Schema as schema_file_name from storage..
    job_config.schema = load_schema_data(bucket, schema_file)

    job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
    job_config.autodetect = True

    job_config.hive_partitioning = HivePartitioningOptions()
    job_config.hive_partitioning.mode = 'AUTO'
    job_config.hive_partitioning.sourceUriPrefix = ('gs://' + bucket_name)
    return job_config


def clear_table(table):
    client.delete_table(table, not_found_ok=True)
    print("Deleted table '{}'.".format(table))


def load_schema_data(bucket, file):
    schema_blob = storage_client.get_bucket(bucket).blob(file)
    table_schema_data = json.loads(schema_blob.download_as_string())

    schema_data = []
    for p in table_schema_data:
        schema_data.append(bigquery.SchemaField(p['name'], p['type']))

    return schema_data


def get_report_data_uri(report_date):
    return "gs://" + bucket_name + "/pickdate=" + report_date + "/data"





def load_data(report_date):

    # Dataset Init..
    trips_ds = client.dataset(ds_id)

    # Get Table full name as Dataset.Table..
    destination_table = utils.get_table_full_name(ds_id, destination_table_name)

    # Clear Table before loading data..
    clear_table(destination_table)

    # Create Report Data Storage Uri..
    uri = get_report_data_uri(report_date)

    # Create Job Config..
    job_config = configure_job(bucket_name, schema_file_name)

    # Run data load job..
    run_job(trips_ds, uri, job_config)

    print("Job finished.")

    # Validate Data in Destination Table..
    # validate_data(trips_ds, destination_table_name)