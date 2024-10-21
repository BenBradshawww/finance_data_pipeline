import psycopg2
from psycopg2.extras import execute_values
import os

def push_to_postgres(**kwargs):
    
    forecast = kwargs['ti'].xcom_pull(task_ids='train_model', key='forecast')

    print(forecast)

    conn = psycopg2.connect(
            host="postgres",
            database=os.getenv('POSTGRES_DATABASE'),
            user=os.getenv('POSTGRES_USERNAME'),
            password=os.getenv('POSTGRES_PASSWORD'),
            port="5432"
        )
    
    cursor = conn.cursor()

    #values = [tuple(row) for row in .itertuples(index=False)]

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
    #execute_values(cursor, query, values)

    #conn.commit()
    
    cursor.close()
    conn.close()