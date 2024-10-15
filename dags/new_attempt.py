from datetime import datetime, timedelta
#from pyspark.sql import SparkSession
import pandas as pd

from airflow import DAG
from airflow.operators.python import PythonOperator

import requests
import logging
import os

import psycopg2

default_args = {
    'owner': 'ben',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

conn = psycopg2.connect(
        host="postgres",
        database=os.getenv('POSTGRES_DATABSE'),
        user=os.getenv('POSTGRES_USERNAME'),
        password=os.getenv('POSTGRES_PASSWORD'),
        port="5432"
    )

#conf = SparkConf().set("spark.ui.port", "4045") 

#spark = SparkSession.config(conf=conf).builder.appName("airflow").getOrCreate()


logging.basicConfig(level=logging.INFO)


def create_table(**kwargs):

    cursor = conn.cursor()

    query = """
        CREATE TABLE IF NOT EXISTS stocks (
            stocks_id SERIAL PRIMARY KEY,
            stocks_updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            stocks_name VARCHAR(10),
            stocks_date VARCHAR(10),
            stocks_timezone TIMESTAMP,
            stocks_open DECIMAL(10, 2),
            stocks_high DECIMAL(10, 2),
            stocks_low DECIMAL(10, 2),
            stocks_close DECIMAL(10, 2),
            stocks_adjusted DECIMAL(10, 2),
            stocks_volume DECIMAL(10, 2),
            stocks_dividend_amount DECIMAL(10, 4),
            stocks_split_coefficient DECIMAL(10, 1)
        );
    """
    
    cursor.execute(query)

    conn.commit()
    
    cursor.close()
    conn.close()


def get_last_date(**kwargs):

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
          stocks_date
        FROM stocks
        ORDER BY
          stocks_date DESC
        LIMIT 1;
    """
    try:
        cursor.execute(query)
        latest_value = cursor.fetchone()
    except:
        latest_value=None
        print('No date found in the table stocks')

    cursor.close()
    conn.close()

    return latest_value[0] if latest_value else None


def working_days_diff(start_date, end_date):

    total_days = (end_date - start_date).days
    working_days = 0

    for i in range(total_days+1):
        day = start_date + timedelta(days=i)
        if day.weekday() < 5:
            working_days += 1

    return working_days


def get_data(**kwargs):

    # Skip get_data if this get_data has been run before
    previous_data = kwargs['ti'].xcom_pull(task_ids='get_data', dag_id=kwargs['dag'].dag_id)

    if previous_data:
        return previous_data

    # Create api arguements
    url = "https://alpha-vantage.p.rapidapi.com/query"

    end_date = datetime.now().date()
    last_date = get_last_date()
    last_date = last_date if last_date else kwargs['dag'].start_date 
    execution_date = kwargs['execution_date']

    days_difference = working_days_diff(
        start_date=last_date,
        end_date=execution_date
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
    kwargs['ti'].xcom_push(key='last_date', value=last_date)


def clean_data(**kwargs):

    # Skip clean_data if this clean_data has been run before
    previous_data = kwargs['ti'].xcom_pull(task_ids='clean_data', dag_id=kwargs['dag'].dag_id)
    last_date = kwargs['ti'].xcom_pull(task_ids='last_date', dag_id=kwargs['dag'].dag_id)

    if previous_data:
        return previous_data
    
    json_list = kwargs['ti'].xcom_pull(task_ids='get_data', key='response_json')
    json_list = json.loads(json_list)

    for stock_data in json_list:
        stock_name = stock_data['Meta Data']['Symbol']
        time_series_data = stock_data['Time Series (Daily)']

        print(stock_name)

    print(response_list)

    df = pd.read_json(response_list)

    df.drop_duplicates(inplace=True)
    df.dropna(inplace=True)

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