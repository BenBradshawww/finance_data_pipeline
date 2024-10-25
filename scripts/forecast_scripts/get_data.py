import psycopg2
import os
import logging
import pandas as pd
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), '../general'))
from general import run_query

columns = [
    'stocks_name', 
    'stocks_date', 
    'stocks_adjusted_close'
]

column_mapping = {
    'stocks_adjusted_close': 'y',
    'stocks_date': 'ds'
}

def get_data(**kwargs):

    query = """
            CREATE TABLE IF NOT EXISTS forecasts (
                forecasts_id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
                forecasts_updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                forecasts_name VARCHAR(50),
                forecasts_date DATE,
                forecasts_adjusted_close DECIMAL(10, 2)
            );
        """
    
    run_query(query=query)

    query = """
        SELECT
            stocks_name,
            stocks_date,
            stocks_adjusted_close
        FROM stocks;
    """

    data = run_query(query=query, is_select=True)
        
    df = pd.DataFrame(data, columns=columns)

    if df.shape[0] == 0:
        raise ValueError(f"No data was returned from the database")
        
    df.rename(columns=column_mapping, inplace=True)
    
    kwargs['ti'].xcom_push(key='df', value=df)
    