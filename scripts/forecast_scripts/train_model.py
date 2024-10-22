import psycopg2
import os
import logging
from prophet import Prophet
import copy

logging.basicConfig(level=logging.INFO)

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
        future = model.make_future_dataframe(periods=30)

        forecast = model.predict(future)

        forecast['forecasts_name'] = stock
    
        forecasts_list.append(copy.deepcopy(forecast))

        logging.info(f'Finished forecasting {stock}')
    
    kwargs['ti'].xcom_push(key='forecasts_list', value=forecasts_list)

