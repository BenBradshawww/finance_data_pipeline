import psycopg2
import os
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), '../general'))
from general import run_query

def create_table(**kwargs):

    query = """
        CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
    """

    run_query(query)

    query = """
        CREATE TABLE IF NOT EXISTS stocks (
            stocks_id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
            stocks_updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            stocks_name VARCHAR(50),
            stocks_date DATE,
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
    
    run_query(query)