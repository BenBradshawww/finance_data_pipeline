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
            stocks_open NUMERIC(15, 3),
            stocks_high NUMERIC(15, 3),
            stocks_low NUMERIC(15, 3),
            stocks_close NUMERIC(15, 3),
            stocks_adjusted_close NUMERIC(15, 3),
            stocks_volume NUMERIC(15, 3),
            stocks_dividend_amount NUMERIC(15, 3),
            stocks_split_coefficient NUMERIC(15, 3)
        );
    """
    
    run_query(query)