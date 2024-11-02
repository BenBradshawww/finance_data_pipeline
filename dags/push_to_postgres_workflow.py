from datetime import datetime, timedelta
import pandas as pd

from airflow import DAG
from airflow.operators.python import PythonOperator

import requests
import logging
import os
import json
import re 
import psycopg2
import sys

#sys.path.append(os.path.join(os.path.dirname(os.getcwd()), 'scripts'))
#sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'scripts'))
sys.path.append('/opt/airflow')

from scripts.push_to_postgres_scripts.create_table import create_table
from scripts.push_to_postgres_scripts.get_last_date import get_last_date
from scripts.push_to_postgres_scripts.get_api_data import get_api_data
from scripts.push_to_postgres_scripts.spark_clean_data import spark_clean_data
from scripts.push_to_postgres_scripts.push_to_postgres import push_to_postgres

default_args = {
    'owner': 'ben',
    'retries': 5,
    'retry_delay': timedelta(minutes=2)
}

with DAG(
    default_args=default_args,
    dag_id='push_to_postgres',
    description='Push to postgres workflow',
    start_date=datetime(2010, 1, 1),
    schedule_interval='0 0 * * *',
    catchup=False
) as dag:

    task0 = PythonOperator(
        task_id='create_table',
        python_callable=create_table,
    )

    task1 = PythonOperator(
        task_id='get_api_data',
        python_callable=get_api_data,
    )

    task2 = PythonOperator(
        task_id='spark_clean_data',
        python_callable=spark_clean_data, 
        provide_context=True,
    )

    task3 = PythonOperator(
        task_id='push_to_postgres',
        python_callable=push_to_postgres, 
    )

    task0 >> task1 >> task2 >> task3