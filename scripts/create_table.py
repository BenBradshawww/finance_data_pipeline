import psycopg2
import os

def create_table(**kwargs):

    try:
        conn = psycopg2.connect(
            host="postgres",
            database=os.getenv('POSTGRES_DATABSE'),
            user=os.getenv('POSTGRES_USERNAME'),
            password=os.getenv('POSTGRES_PASSWORD'),
            port="5432"
        )


        cursor = conn.cursor()

        query = """
            CREATE TABLE IF NOT EXISTS stocks (
                stocks_id SERIAL PRIMARY KEY,
                stocks_updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                stocks_name VARCHAR(10),
                stocks_date VARCHAR(10),
                stocks_timezone TIMESTAMP,
                stocks_open DECIMAL(10, 2),
                stocks_high DECIMAL(10, 2),
                stocks_low DECIMAL(10, 2),
                stocks_close DECIMAL(10, 2),
                stocks_adjusted_close DECIMAL(10, 2),
                stocks_volume DECIMAL(10, 2),
                stocks_dividend_amount DECIMAL(10, 4),
                stocks_split_coefficient DECIMAL(10, 1)
            );
        """
        
        cursor.execute(query)

        conn.commit()
    except psycopg2.Error as e:
        print(f"Error: {e}")
        conn.rollback() 
        raise ValueError('Connection Issue')
    finally:
        cursor.close()
        conn.close()