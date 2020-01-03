'''clear any erroneous data, state the reason why you think there is an error in that data(e.g wrong gps readings etc).
●	Create HexagonId columns from pickup and dropoff lat, long values.
○	Hint: HexagonIds can be created using Uber’s hexagonal indexing library.  (https://nbviewer.jupyter.org/github/uber/h3-py-notebooks/blob/master/Usage.ipynb)
○	Hint: Look into creating and using UDF on BigQuery
●	Create daypart column from pickup_datetime:
○	06:00-12:00 Morning
○	12:00-17:59 Noon
○	18:00 - 23:59 Evening
○	00:00- 05:59 Night
'''

# used h3cy since no need for cmake etc..
import h3cy._h3 as h3

from google.cloud import bigquery
import utils

client = bigquery.Client()
ds_id = 'new_york_taxi_trips'
destination_table_name = 'trips_data'
report_table_name = 'trips_report_data'

bucket_name = 'sample_data'
schema_file_name = 'trips_schema.json'

# see ref @ https://uber.github.io/h3/#/documentation/core-library/resolution-table
# 25 meters approximately
h3_resolution_value = 11

popular_results_count=10


def load_filtered_data(full_table_name):
    '''
    ######  ETL CLEANING INCLUDES  ######
    trip_distance MUST BE BIGGER THAN 0
    dropoff_longitude MUST BE BIGGER THAN 0
    dropoff_latitude MUST BE BIGGER THAN 0
    pickup_longitude MUST BE BIGGER THAN 0
    pickup_latitude MUST BE BIGGER THAN 0
    passenger_count  MUST BE BIGGER THAN 0
    fare_amount  MUST BE BIGGER THAN 0
    pickup_hexagon  AND  drop-off SHOULD NOT BE THE SAME FOR A VALID TRIP (will be filtered after hexagons are computed.) //TO DOUBLE CHECK trip_distance..
    '''

    query_text = """
    CREATE TEMP FUNCTION getDatePartAsString(x TIMESTAMP)
    RETURNS STRING
    LANGUAGE js AS \"""
      var d = new Date(x);
      var h=d.getUTCHours();
      if ( h>=6 &&  h < 12){return 'Morning';} 
      else if(h>=12 && h<18){return 'Noon';}
      else if(h>=18 && h<24){return 'Evening';}
      else {return 'Night';}
      \""";
    SELECT *,getDatePartAsString(pickup_datetime) as datepart FROM `""" + full_table_name + """`WHERE trip_distance > 0 AND dropoff_longitude != 0 AND dropoff_latitude != 0 AND pickup_longitude != 0 AND pickup_latitude != 0 AND passenger_count > 0 AND fare_amount > 0 """

    return client.query(query_text).result().to_dataframe()


def create_hexagons(df):
    df['pickup_hex'] = df.apply(
        lambda row: h3.geo_to_h3(row['pickup_latitude'], row['pickup_longitude'], h3_resolution_value), axis=1)
    df['dropoff_hex'] = df.apply(
        lambda row: h3.geo_to_h3(row['dropoff_latitude'], row['dropoff_longitude'], h3_resolution_value), axis=1)

    return df


def load_data_from_gbq(dataset_name, table_name):
    # Get Table Name used for creating reports..
    table_name = utils.get_table_full_name(dataset_name, table_name)

    # Load BigQuery query to table and return as Pandas.DataFrame
    df_trips = load_filtered_data(table_name)

    # Add Hexagons Data To DataFrame
    create_hexagons(df_trips)

    return df_trips


def load_data_to_gbq(df, table_full_name, schema_data):
    job_config = bigquery.LoadJobConfig(
        schema=schema_data,
        write_disposition="WRITE_TRUNCATE",
    )
    job = client.load_table_from_dataframe(
        df, table_full_name, job_config=job_config
    )
    job.result()


def load_data_to_gbq(df, table_full_name, schema_data):
    job_config = bigquery.LoadJobConfig(
        schema=schema_data,
        write_disposition="WRITE_TRUNCATE",
    )
    job = client.load_table_from_dataframe(
        df, table_full_name, job_config=job_config
    )
    job.result()


def get_destination_schema():
    schema_data = utils.load_schema_data(bucket_name, schema_file_name)
    schema_data.append(bigquery.SchemaField('pickup_hex', bigquery.enums.SqlTypeNames.STRING))
    schema_data.append(bigquery.SchemaField('dropoff_hex', bigquery.enums.SqlTypeNames.STRING))
    return schema_data


def filter_reload_data(dataset_name, table_name):
    trips_data = load_data_from_gbq(dataset_name, table_name)
    new_schema_data = get_destination_schema()

    load_data_to_gbq(trips_data, utils.get_table_full_name(dataset_name, table_name), new_schema_data)


def get_popular_hexs(full_table_name, field_name, report_type,limit):
    query_text = "SELECT " + field_name + " as hexagonId, COUNT(" + field_name + ") as count, '" + report_type + "' AS type FROM `" + full_table_name + "` GROUP BY " + field_name + " ORDER BY count DESC LIMIT "+str(limit)
    return client.query(query_text).result().to_dataframe()

def get_popular_hex_routes(full_table_name, report_type,limit):
    query_text = "SELECT CONCAT(pickup_hex, '-', dropoff_hex) AS hexagonId, COUNT(CONCAT(pickup_hex, '-', dropoff_hex)) AS count, '"+ report_type+"' AS type FROM `"+full_table_name+"` GROUP BY hexagonId ORDER BY count DESC LIMIT "+str(limit)
    return client.query(query_text).result().to_dataframe()


def start_etl():
    filter_reload_data(ds_id, destination_table_name)

    table_full_name = utils.get_table_full_name(ds_id, destination_table_name)

    popular_pickups = get_popular_hexs(table_full_name, 'pickup_hex', 'pickup',popular_results_count)
    popular_dropoff = get_popular_hexs(table_full_name, 'dropoff_hex', 'dropoff',popular_results_count)
    popular_routes = get_popular_hex_routes(table_full_name, 'route',popular_results_count)

    popular_pickups = popular_pickups.append(popular_dropoff, ignore_index=True)
    popular_pickups = popular_pickups.append(popular_routes, ignore_index=True)

    report_schema_data = [bigquery.SchemaField('hexagonId', bigquery.enums.SqlTypeNames.STRING),
                          bigquery.SchemaField('count', bigquery.enums.SqlTypeNames.INTEGER),
                          bigquery.SchemaField('type', bigquery.enums.SqlTypeNames.STRING)]

    report_table_full_name = utils.get_table_full_name(ds_id, report_table_name)

    load_data_to_gbq(popular_pickups, report_table_full_name, report_schema_data)

    print('done!')
