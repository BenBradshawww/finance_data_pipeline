import psycopg2
import logging
import os

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_last_date(**kwargs):

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
        logging.info('No id found in the table stocks')

    cursor.close()
    conn.close()

    return latest_value[0] if latest_value else None