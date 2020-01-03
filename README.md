# gcp
# writtenBy=scubamemo
# This document briefly describes how to deploy this application on Google Cloud Platform and severals steps that needs to be completed initially.

#### SETUP ####
1. Create a GCP account
2. Create a GCP Project
3. Create a service account capable of accessing BigQuery,Storage,Composer services.
4. By using gcp cli;
# Run the above query to clone the necessary partitioned dataTable on project resources for the given DateRange(First week of March of 2014 in this case.)
bq query \
--destination_table new_york_taxi_trips.tlc_green_trips_2014_partitioned \
--time_partitioning_field pickup_datetime \
--use_legacy_sql=false \
'SELECT * EXCEPT(pickup_datetime,dropoff_datetime),
CAST(pickup_datetime AS TIMESTAMP) AS pickup_datetime,
CAST(dropoff_datetime AS TIMESTAMP) AS dropoff_datetime
FROM `bigquery-public-data.new_york_taxi_trips.tlc_green_trips_2014` WHERE pickup_datetime BETWEEN "2014-03-01" and "2014-03-08" AND dropoff_datetime BETWEEN "2014-03-01" and "2014-03-08"'

5. 
# Next run this command above to extract the schema of the dataTable and include it in your project(if necessary, already supplied in the repository!)
bq show \
--schema \
--format=prettyjson \
new_york_taxi_trips.tlc_green_trips_2014_partitioned > /trips_schema.json

6. Create a bucket in Google Cloud Storage and upload your partitioned dataset in the Google Cloud Storage.
Folder structure should be compatible with Hive Partition Structure based on column 'pickup_date'
A sample structure is as follows;
https://storage.cloud.google.com/sample_data/pickdate%3D2014-03-01/data?folder=true&organizationId=true

7. Next step is to create an environment to use Google Composer Managed Service.
8. Update pypi packages mentioned in requirements.txt on environment configuration in order to run the code in the repository.
9. By using the Admin>Pools menu, please set Pool value to 1 not to waste unnecessary resources since the aim of this project is just to create a report running once a day.
10. Copy all python code files to the "dags" folder available in cloud storage created for the Composer service.
11. After several minutes following the file upload process to the "dags" directory, a new DAG process should appear in the Dag List of Google Composer WebUI (AirFlow Web GUI)
12. You can trigger the Dag by hitting the play button to make it run.
13. The report dataTable will be available as trips_report_data(default) and won't be cleaned automatically.
14. The user should also clear the trips_data(default) table and needs to be deleted. 
                                                            

#### CUSTOMIZATIONS & ASSUMPTIONS ####

Used variables in source code are defined at the top of each source code file. You may need to update the values correspondingly since environmental,storage and project settings may vary.

DAG orchestration code file uses:
'start_date': datetime.datetime(2020, 1, 3) as default arguments and needs to be changed according to requirements.

A report_date parameter is required by the  LoadDataJob function (The first step of the workflow) and set as load_data('2014-03-01') in master_orchestrate_dad.py @ line 23 and should be updated. 


h3_resolution_value = 11 is set to unify any coordinate within 25 meters radius 
# see ref @ https://uber.github.io/h3/#/documentation/core-library/resolution-table
# 25 meters approximately

popular_results_count=10
# The number of popular hexagons and routes are limited to 10, you may change it depending on your requirements.


# In Data Cleaning process inside EtlJob is done by filtering invalid data using BigQuery's sql query structure.

trip_distance MUST BE BIGGER THAN 0 (pickup_hexagon  AND  drop-off SHOULD NOT BE THE SAME FOR A VALID TRIP)
dropoff_longitude MUST BE BIGGER THAN 0
dropoff_latitude MUST BE BIGGER THAN 0
pickup_longitude MUST BE BIGGER THAN 0
pickup_latitude MUST BE BIGGER THAN 0
passenger_count  MUST BE BIGGER THAN 0
fare_amount  MUST BE BIGGER THAN 0

Trips that do not satisfy the above conditions are filtered and considered invalid/dirty data.

##### GENERAL ARCHITECTURE #####

This basic workflow application uses BigQuery to generate reports and store data by cloning the appropriate data partition from Google Cloud Storage bucket mentioned above.

There is a master DAG file responsible for orchestrating the subProcesses in this flow.
An event is automatically fired after each task is completed that triggers the sooner tasks in a chain manner.
There are 2 simple jobs in this flow;
1 - LoadDataJob (which copies hive partitioned data on to BigQuery Table)
2 - EtlJob (which is responsible for cleansing the data(wrong coordinates,etc..

The data flows as shown below;
Storage -> BigQuery <--> Cloud Composer



##### ESTIMATED COST OF CURRENT ARCHITECTURE(monthly) #####

Several optimizations like employing "Cloud Functions" can be made to reduce the cost of using a managed service (Cloud Composer) in this case. Depending on the location and infrastructure, the following table is expected to be charged at the end of each month.

Jan 1 – 31, 2020
Cloud Storage Class A Request Regional Storage: 258435 Counts [Currency conversion: USD to TRY using rate 5.958]
TRY 7.55
Jan 1 – 31, 2020
Cloud Composer Cloud Composer vCPU time in Iowa: 100.944 Hours [Currency conversion: USD to TRY using rate 5.958]
TRY 44.51
Jan 1 – 31, 2020
Cloud Composer Cloud Composer network egress from Iowa: 6.565 Gibibytes [Currency conversion: USD to TRY using rate 5.958]
TRY 6.10
Jan 1 – 31, 2020
Cloud Composer Cloud Composer SQL vCPU time in Iowa: 100.278 Hours [Currency conversion: USD to TRY using rate 5.958]
TRY 74.69
Jan 1 – 31, 2020
Cloud Composer Cloud Composer Data storage in Iowa: 1.352 Gibibyte-months [Currency conversion: USD to TRY using rate 5.958]




  
