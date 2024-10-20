from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys

sys.path.append('/opt/airflow')

from scripts.forecast_scripts.get_data import get_data
from scripts.forecast_scripts.train_model import train_model

default_args = {
    'owner': 'ben',
    'retries': 5,
    'retry_delay': timedelta(minutes=2)
}

with DAG(
    default_args=default_args,
    dag_id='forecast',
    description='Forecasting workflow'
) as dag:

    task0 = PythonOperator(
        task_id='get_data',
        python_callable=get_data,
    )

    task1 = PythonOperator(
        task_id='train_model',
        python_callable=train_model,
    )

    task0 >> task1