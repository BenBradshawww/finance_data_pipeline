import psycopg2
import os
import logging

logging.basicConfig(level=logging.INFO)

def run_query(query):

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
        cursor.execute(query)
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

def recreate_forecasts_table(**kwargs):

    query = """
        DROP TABLE IF EXISTS forecasts;
    """
    
    run_query(query)

    query = """
        CREATE TABLE IF NOT EXISTS forecasts (
            forecasts_id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
            forecasts_updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            forecasts_name VARCHAR(50),
            forecasts_date DATE,
            forecasts_adjusted_close DECIMAL(10, 2)
        );
    """

    run_query(query)