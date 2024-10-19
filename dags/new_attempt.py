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
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'scripts'))
sys.path.append('/opt/airflow/scripts')

from create_table import create_table
from get_last_date import get_last_date
from get_data import get_data
from clean_data import clean_data
from push_to_warehouse import push_to_warehouse

default_args = {
    'owner': 'ben',
    'retries': 5,
    'retry_delay': timedelta(minutes=2)
}

with DAG(
    default_args=default_args,
    dag_id='real_dag',
    description='My first dag',
    start_date=datetime(2024, 10, 8),
    schedule='0 0 * * *',
    catchup=False
) as dag:

    task0 = PythonOperator(
        task_id='create_table',
        python_callable=create_table,
    )

    task1 = PythonOperator(
        task_id='get_data',
        python_callable=get_data,
    )

    task2 = PythonOperator(
        task_id='clean_data',
        python_callable=clean_data, 
    )

    task3 = PythonOperator(
        task_id='push_to_warehouse',
        python_callable=push_to_warehouse, 
    )

    task0 >> task1 >> task2 >> task3