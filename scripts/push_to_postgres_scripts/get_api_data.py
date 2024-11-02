from airflow.exceptions import AirflowFailException
from scripts.push_to_postgres_scripts.get_last_date import get_last_date
from datetime import datetime, timedelta
import os
import requests

def working_days_diff(start_date, end_date):

    total_days = (end_date - start_date).days
    working_days = 0

    for i in range(total_days+1):
        day = start_date + timedelta(days=i)
        if day.weekday() < 5:
            working_days += 1

    return working_days


def get_api_data(**kwargs):

    previous_data = kwargs['ti'].xcom_pull(task_ids='get_api_data', dag_id=kwargs['dag'].dag_id)

    if previous_data:
        return previous_data

    end_date = datetime.now().date()
    last_date = get_last_date()
    last_date = last_date if last_date else kwargs['dag'].start_date.date()
    execution_date = kwargs['execution_date'].date()

    days_difference = working_days_diff(
        start_date=last_date,
        end_date=execution_date
    )

    if days_difference <= 1:
        raise AirflowFailException("No difference in the last date in database and execution date.")

    
    OUTPUT_SIZE = 'compact' if days_difference < 100 else 'full'
    API_KEY = os.getenv('ALPHA_VANTAGE_API_KEY')
    URL = os.getenv('ALPHA_VANTAGE_URL')
    
    STOCK_SYMBOLS = ['MSFT']
    FUNCTION = 'TIME_SERIES_DAILY_ADJUSTED'
    
    json_results = []

    for STOCK_SYMBOL in STOCK_SYMBOLS:

        querystring = {
            "function":FUNCTION,
            "symbol":STOCK_SYMBOL,
            "outputsize":OUTPUT_SIZE,
            "datatype":"json"
        }

        headers = {
            "x-rapidapi-key": API_KEY,
            "x-rapidapi-host": "alpha-vantage.p.rapidapi.com"
        }

        response = requests.get(URL, headers=headers, params=querystring)

        json_results.append(response.json())
        
    kwargs['ti'].xcom_push(key='response_json', value=json_results)
    kwargs['ti'].xcom_push(key='last_date', value=last_date.isoformat())