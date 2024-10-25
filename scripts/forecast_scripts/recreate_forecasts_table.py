import psycopg2
import os
import logging
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), '../general'))
from general import run_query

logging.basicConfig(level=logging.INFO)

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
        forecasts_yhat DECIMAL(10, 6),
        forecasts_trend DECIMAL(10, 6),
        forecasts_yhat_lower DECIMAL(10, 6),
        forecasts_yhat_upper DECIMAL(10, 6),
        forecasts_trend_lower DECIMAL(10, 6),
        forecasts_trend_upper DECIMAL(10, 6),
        forecasts_additive_terms DECIMAL(10, 6),
        forecasts_additive_terms_lower DECIMAL(10, 6),
        forecasts_additive_terms_upper DECIMAL(10, 6),
        forecasts_weekly DECIMAL(10, 6),
        forecasts_weekly_lower DECIMAL(10, 6),
        forecasts_weekly_upper DECIMAL(10, 6),
        forecasts_multiplicative_terms DECIMAL(10, 6),
        forecasts_multiplicative_terms_lower DECIMAL(10, 6),
        forecasts_multiplicative_terms_upper DECIMAL(10, 6)
    );
    """

    run_query(query)