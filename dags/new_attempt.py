from datetime import datetime, timedelta
from pyspark.sql import SparkSession

from airflow import DAG
from airflow.operators.python import PythonOperator

import requests
import logging
import os

default_args = {
    'owner': 'ben',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

logging.basicConfig(level=logging.INFO)

def get_last_date():

    conn = psycopg2.connect(
        host="postgres",
        database=os.getenv('POSTGRES_DATABSE'),
        user=os.getenv('POSTGRES_USERNAME'),
        password=os.getenv('POSTGRES_PASSWORD'),
        port="5432"
    )

    cursor = conn.cursor()

    query = """
        SELECT
          stock_date
        FROM stocks
        ORDER BY
          stock_date DESC
        LIMIT 1;
    """
    
    cursor.execute(query)
    
    latest_value = cursor.fetchone()
    
    cursor.close()
    conn.close()

    return latest_value[0] if latest_value else None


def working_days_diff(start_date, end_date):
    total_days = (end_date - start_date).days()
    working_days = 0

    for i in range(total_days+1):
        day = start + timedelta(days=i)
        if day.weekday() < 5:
            working_days += 1

    return working_days

def get_data(**kwargs):

    # Skip get_data if this get_data has been run before
    previous_data = ti.xcom_pull(task_ids='get_data', dag_id=kwargs['dag'].dag_id)

    if previous_data:
        return previous_data

    # Create api arguements
    url = "https://alpha-vantage.p.rapidapi.com/query"

    end_date = datetime.now().date()
    last_date = get_last_date()
    start_date = last_date if last_date else kwargs['dag'].start_date 

    days_difference = working_days_diff(
        start_date=start_date,
        execution_date=execution_date
    )

    OUTPUT_SIZE = 'compact' if days_difference < 100 else 'full'
    API_KEY = os.getenv('ALPHA_VANTAGE_API_KEY')
    STOCK_SYMBOLS = ['MSFT']
    FUNCTION = 'TIME_SERIES_DAILY_ADJUSTED'
    
    json_results = []

    for STOCK_SYMBOL in STOCK_SYMBOLS:

        querystring = {
            "function":"TIME_SERIES_DAILY_ADJUSTED",
            "symbol":STOCK_SYMBOL,
            "outputsize":"compact",
            "datatype":"json"
        }

        headers = {
            "x-rapidapi-key": API_KEY,
            "x-rapidapi-host": "alpha-vantage.p.rapidapi.com"
        }

        response = requests.get(url, headers=headers, params=querystring)

        json_results.append(response.json())
    
    kwargs['ti'].xcom_push(key='response_json', value=json_results)


def clean_data(**kwargs):

    # Skip clean_data if this clean_data has been run before
    previous_data = ti.xcom_pull(task_ids='clean_data', dag_id=kwargs['dag'].dag_id)

    if previous_data:
        return previous_data
    
    response_list = kwargs['ti'].xcom_pull(task_ids='get_data', key='response_json')

    print(response_list)

    df = spark.createDataFrame(response_list)

    df_no_duplicates = df.dropDuplicates()
    df_no_nas = df_no_duplicates.na.drop()

    last_date = get_last_date()
    start_date = last_date if last_date else kwargs['dag'].start_date 

    kwargs['ti'].xcom_push(key='df', value=df_no_nas)

def push_to_warehouse(**kwargs):
    
    df = kwargs['ti'].xcom_pull(task_ids='clean_data', key='df')

    kwargs['ti'].xcom_push(key='df', value=df_no_nas)
    

with DAG(
    default_args=default_args,
    dag_id='real_dag',
    description='My first dag',
    start_date=datetime(2024, 10, 8),
    schedule='0 0 * * *',
    catchup=False
) as dag:

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

    task1 >> task2 >> task3