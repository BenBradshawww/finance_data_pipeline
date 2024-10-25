import psycopg2
import os
import logging
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), '../general'))
from general import run_query

def drop_table(**kwargs):
    query = """
        DROP TABLE IF EXISTS stocks;
    """
    
    run_query(query)
        