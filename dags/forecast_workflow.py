from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys

sys.path.append('/opt/airflow')

from scripts.forecast_scripts.get_data import get_data
from scripts.forecast_scripts.train_model import train_model
from scripts.forecast_scripts.recreate_forecasts_table import recreate_forecasts_table
from scripts.forecast_scripts.push_forecasts import push_forecasts

default_args = {
    'owner': 'ben',
    'retries': 5,
    'retry_delay': timedelta(minutes=2)
}

with DAG(
    default_args=default_args,
    dag_id='forecast',
    description='Forecasting workflow',
    start_date=datetime(2024, 1, 1),
    schedule_interval='0 2 * * *',
    catchup=False
) as dag:

    task0 = PythonOperator(
        task_id='get_data',
        python_callable=get_data,
    )

    task1 = PythonOperator(
        task_id='train_model',
        python_callable=train_model,
    )

    task2 = PythonOperator(
        task_id='recreate_forecasts_table',
        python_callable=recreate_forecasts_table,
    )

    task3 = PythonOperator(
        task_id='push_forecasts',
        python_callable=push_forecasts,
    )

    task0 >> task1 >> task2 >> task3