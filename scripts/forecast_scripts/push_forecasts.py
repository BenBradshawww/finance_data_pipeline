import psycopg2
from psycopg2.extras import execute_values
import os
import logging

logging.basicConfig(level=logging.INFO)

def run_query(query, is_select=False):

    conn = psycopg2.connect(
            host="postgres",
            database=os.getenv('POSTGRES_DATBASE'),
            user=os.getenv('POSTGRES_USERNAME'),
            password=os.getenv('POSTGRES_PASSWORD'),
            port="5432"
    )

    cursor = conn.cursor()

    try:
        logging.info(f'Running query: {query}')
        if not is_insert:
            cursor.execute(query)
            logging.info(f'Query run successfully')
            conn.commit()
        else:
            execute_values(cursor, is_insert, query, values)

    except psycopg2.OperationalError as e:
        logging.error(f"Operational Error: {e.pgcode} - {e.pgerror}")
        conn.rollback()
        raise ValueError(f"Operational issue occurred: {e.pgcode}")

    except psycopg2.IntegrityError as e:
        logging.error(f"Integrity Error: {e.pgcode} - {e.pgerror}")
        conn.rollback()
        raise ValueError(f"Integrity issue occurred: {e.pgcode}")

    except psycopg2.Error as e:
        logging.error(f"Database Error: {e.pgcode} - {e.pgerror}")
        conn.rollback()
        raise ValueError(f"Database issue occurred: {e.pgcode}")
    
    except Exception as e:
        logging.error(f"Unexpected error: {str(e)}")
        conn.rollback()
        raise RuntimeError("An unexpected error occurred")
    
    finally:
        cursor.close()
        conn.close()


def push_forecasts(**kwargs):
    
    forecast = kwargs['ti'].xcom_pull(task_ids='train_model', key='forecast')

    print(forecast)
    values = [tuple(row) for row in forecast.itertuples(index=False)]

    query = """
        INSERT INTO forecasts (
            forecasts_name,
            forecasts_date, 
            forecasts_m
        ) VALUES %s
    """

    run_query(query)
