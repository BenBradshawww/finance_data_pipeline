import psycopg2
import os
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def drop_table(**kwargs):

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
            DROP TABLE IF EXISTS stocks;
        """
        
        cursor.execute(query)

        conn.commit()
    except psycopg2.Error as e:
        print(f"Error: {e}")
        conn.rollback() 
        raise ValueError('Connection Issue')
    finally:
        logging.info('Table stocks was dropped')
        cursor.close()
        conn.close()