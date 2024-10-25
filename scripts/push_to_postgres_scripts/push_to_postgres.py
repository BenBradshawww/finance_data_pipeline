import psycopg2
from psycopg2.extras import execute_values
import os
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), '../general'))
from general import run_query

def push_to_postgres(**kwargs):
    
    df = kwargs['ti'].xcom_pull(task_ids='clean_data', key='df')

    values = [tuple(row) for row in df.itertuples(index=False)]

    query = """
        INSERT INTO stocks (
            stocks_name,
            stocks_date,
            stocks_timezone,
            stocks_open,
            stocks_high,
            stocks_low,
            stocks_close,
            stocks_adjusted_close,
            stocks_volume,
            stocks_dividend_amount,
            stocks_split_coefficient
        ) VALUES %s
    """

    run_query(query, is_insert=True, values=values)