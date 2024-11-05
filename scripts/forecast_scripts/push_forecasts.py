import psycopg2
from psycopg2.extras import execute_values
import os
import logging
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), '../general'))
from general import run_query

def push_forecasts(**kwargs):
    
    forecasts_list = kwargs['ti'].xcom_pull(task_ids='train_model', key='forecasts_list')
    
    all_stocks_values = []
    for forecast in forecasts_list:
        stock_values = [tuple(row) for row in forecast.itertuples(index=False)]
        all_stocks_values.extend(stock_values)

    query = """
        INSERT INTO forecasts (
            forecasts_name,
            forecasts_field,
            forecasts_date, 
            forecasts_yhat,
            forecasts_trend,
            forecasts_yhat_lower,
            forecasts_yhat_upper,
            forecasts_trend_lower,
            forecasts_trend_upper,
            forecasts_additive_terms,
            forecasts_additive_terms_lower,
            forecasts_additive_terms_upper,
            forecasts_weekly,
            forecasts_weekly_lower,
            forecasts_weekly_upper,
            forecasts_multiplicative_terms,
            forecasts_multiplicative_terms_lower,
            forecasts_multiplicative_terms_upper
        ) VALUES %s
    """

    run_query(query, is_insert=True, values=all_stocks_values)
