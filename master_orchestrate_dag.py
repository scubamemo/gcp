from __future__ import print_function

import datetime

from airflow import models
from airflow.operators import python_operator

from etl_job import start_etl
from load_to_bq import load_data

default_dag_args = {
    'start_date': datetime.datetime(2020, 1, 3)
}

with models.DAG(
        'firefly-master-workflow-v2',
        schedule_interval=datetime.timedelta(days=1),
        default_args=default_dag_args) as dag:

    def load():
        import logging
        logging.info('Started loading process!')
        load_data('2014-03-01')

    def etl():
        import logging
        logging.info('Started etl process!')
        start_etl()

    load_job = python_operator.PythonOperator(
        task_id='load_data',
        python_callable=load)

    etl_job = python_operator.PythonOperator(
        task_id='etl_data',
        python_callable=etl)

    load_job >> etl_job
