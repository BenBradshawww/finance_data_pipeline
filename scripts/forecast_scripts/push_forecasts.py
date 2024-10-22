import psycopg2
from psycopg2.extras import execute_values
import os
import logging

logging.basicConfig(level=logging.INFO)

def run_query(query, is_insert=False, values=None):

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
        else:
            execute_values(cursor, query, values)
        logging.info(f'Query run successfully')
        conn.commit()

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
    
    forecasts_list = kwargs['ti'].xcom_pull(task_ids='train_model', key='forecasts_list')

    for forecast in forecasts_list:
        print(forecast)
        values = [tuple(row) for row in forecast.itertuples(index=False)]
    print(values)

    query = """
        INSERT INTO forecasts (
            forecasts_name,
            forecasts_date, 
            forecasts_yhat,
            forecasts_trend,
            forecasts_yhat_lower,
            forecasts_yhat_upper,
            forecasts_trend_lower,
            forecasts_trend_upper,
            forecasts_additive_terms,
            forecasts_additive_terms_lower,
            forecasts_additive_terms_upper,
            forecasts_weekly,
            forecasts_weekly_lower,
            forecasts_weekly_upper,
            forecasts_multiplicative_terms,
            forecasts_multiplicative_terms_lower,
            forecasts_multiplicative_terms_upper
        ) VALUES %s
    """

    run_query(query, is_insert=True, values=values)
