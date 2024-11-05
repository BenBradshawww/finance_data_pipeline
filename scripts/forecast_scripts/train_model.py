import psycopg2
import os
import logging
from prophet import Prophet

logging.basicConfig(level=logging.INFO)

column_ordering = [
    'name',
    'field',
    'ds',
    'yhat',
    'trend',
    'yhat_lower',
    'yhat_upper',
    'trend_lower',
    'trend_upper',
    'additive_terms',
    'additive_terms_lower',
    'additive_terms_upper',
    'weekly',
    'weekly_lower',
    'weekly_upper',
    'multiplicative_terms',
    'multiplicative_terms_lower',
    'multiplicative_terms_upper'
]

def train_model(**kwargs):

    df = kwargs['ti'].xcom_pull(task_ids='get_data', key='df')
    unique_stocks = df['stocks_name'].unique().tolist()

    forecasts_list = []

    for stock in unique_stocks:
        logging.info(f'Training {stock} model')

        stock_df = df[df['stocks_name']==stock]

        model = Prophet()

        model.fit(stock_df)
        logging.info(f'Finished training {stock} model')
        logging.info(f'Forecasting {stock}')
        future = model.make_future_dataframe(periods=5)

        forecast = model.predict(future)

        forecast['name'] = stock
        forecast['field'] = 'close'
        forecast = forecast[column_ordering]
    
        forecasts_list.append(forecast)

        logging.info(f'Finished forecasting {stock}')
    
    kwargs['ti'].xcom_push(key='forecasts_list', value=forecasts_list)

