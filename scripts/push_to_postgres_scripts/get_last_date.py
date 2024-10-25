import psycopg2
import logging
import os
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), '../general'))
from general import run_query

def get_last_date(**kwargs):

    query = """
        SELECT
          stocks_date
        FROM stocks
        ORDER BY
          stocks_date DESC
        LIMIT 1;
    """
    
    latest_value = run_query(query)

    return latest_value[0] if latest_value else None