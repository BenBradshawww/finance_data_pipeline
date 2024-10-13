from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

def hello_world():
    print('Hello World!')

default_args = {
    'owner': 'ben',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

# Task 1: Push names and a greeting to XCom
def get_names(**kwargs):
    names = ["Alice", "Bob"]
    greeting = "Hello"
    
    kwargs['ti'].xcom_push(key='names', value=names)
    kwargs['ti'].xcom_push(key='greeting', value=greeting)


# Task 2: Pull names and greeting from XCom and print a message for each name
def say_hello(**kwargs):
    names = kwargs['ti'].xcom_pull(task_ids='get_names', key='names')
    greeting = kwargs['ti'].xcom_pull(task_ids='get_names', key='greeting')
    
    for name in names:
        print(f"{greeting}, {name}!")
    

with DAG(
    default_args=default_args,
    dag_id='first_dag',
    description='My first dag',
    start_date=datetime(2024, 10, 8),
    schedule_interval='0 0 * * *',
    catchup=False
) as dag:

    task1 = PythonOperator(
        task_id='get_names',
        python_callable=get_names, 
        provide_context=True,
    )

    task2 = PythonOperator(
        task_id='say_hello',
        python_callable=say_hello, 
        provide_context=True,
    )

    task1 >> task2