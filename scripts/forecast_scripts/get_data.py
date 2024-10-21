import psycopg2
import os
import logging
import pandas as pd

columns = [
    'stocks_name', 
    'stocks_date', 
    'stocks_open', 
    'stocks_high', 
    'stocks_low', 
    'stocks_close', 
    'stocks_adjusted_close', 
    'stocks_volume', 
    'stocks_dividend_amount', 
    'stocks_split_coefficient'
]

logging.basicConfig(level=logging.ERROR)

def get_data(**kwargs):

    try:
        conn = psycopg2.connect(
            host="postgres",
            database=os.getenv('POSTGRES_DATBASE'),
            user=os.getenv('POSTGRES_USERNAME'),
            password=os.getenv('POSTGRES_PASSWORD'),
            port="5432"
        )

        cursor = conn.cursor()

        query = """
            SELECT
                stocks_name,
                stocks_date,
                stocks_adjusted_close
            FROM stocks;
        """
        
        cursor.execute(query)

        data = cursor.fetchall()
        df = pd.DataFrame(data, columns = columns)

        if df.shape[0] == 0:
            raise ValueError(f"No data was returned from the database")
        
        df.rename({'stocks_adjusted_close': 'y'}, inplace=True)

        kwargs['ti'].xcom_push(key='df', value=df)

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

    finally:
        cursor.close()
        conn.close()