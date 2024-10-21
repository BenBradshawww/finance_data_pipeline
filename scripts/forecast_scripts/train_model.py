import psycopg2
import os
import logging
from prophet import Prophet

def train_model(**kwargs):

    df = kwargs['ti'].xcom_pull(task_ids='get_data', key='df')

    model = Prophet()

    model.fit(df)

    future = model.make_future_dataframe(periods=30)

    forecast = model.predict(future)

    kwargs['ti'].xcom_push(key='forecast', value=forecast)

