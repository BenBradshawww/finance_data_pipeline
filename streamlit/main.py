import streamlit as st
import plotly.graph_objects as go
import pandas as pd
import numpy as np
import sys
import os

sys.path.append(os.path.join(os.path.dirname(__file__), '../scripts'))
from general.general import run_query

def get_stocks():

    query = f"""
      SELECT DISTINCT
        stocks_name
      FROM stocks;
    """

    data = run_query(query, is_select=True)

    return data[0]

def get_relevant_data(stock_name):

    query = f"""
      SELECT DISTINCT
        stocks_date,
        stocks_adjusted_close
      FROM stocks
      WHERE stocks_name = '{stock_name}';
    """

    current_data = run_query(query, is_select=True)

    query = f"""
      SELECT DISTINCT
        forecasts_date,
        forecasts_yhat,
        forecasts_yhat_lower,
        forecasts_yhat_upper
      FROM forecasts
      WHERE forecasts_name = '{stock_name}';
    """

    forecast_data = run_query(query, is_select=True)

    return (current_data, forecast_data)

    
def plot_data(current_data, forecast_data):

    current_data_df = pd.DataFrame(
        current_data, 
        columns = ['date', 'close_price']
    )

    forecast_df = pd.DataFrame(
        forecast_data, 
        columns = ['date', 'close_price', 'close_price_lower', 'close_price_upper']
    )

    forecast_df = forecast_df[forecast_df['date']>current_data_df['date'].max()]

    current_data_df.sort_values(
        by=['date'],
        ascending=True,
        inplace=True
    )

    current_data_df['date'] = pd.to_datetime(current_data_df['date'], errors='coerce')
    current_data_df['close_price'] = pd.to_numeric(current_data_df['close_price'], errors='coerce')

    forecast_df['date'] = pd.to_datetime(forecast_df['date'], errors='coerce')
    forecast_df['close_price'] = pd.to_numeric(forecast_df['close_price'], errors='coerce')
    forecast_df['close_price_lower'] = pd.to_numeric(forecast_df['close_price_lower'], errors='coerce')
    forecast_df['close_price_upper'] = pd.to_numeric(forecast_df['close_price_upper'], errors='coerce')

    row_id = current_data_df['date'].idxmax()

    row_df = pd.DataFrame({
        'date': [current_data_df['date'][row_id]],
        'close_price': [current_data_df['close_price'][row_id]],
        'close_price_lower': [current_data_df['close_price'][row_id]],
        'close_price_upper': [current_data_df['close_price'][row_id]],
        }
    )

    updated_forecast_df = pd.concat([forecast_df, row_df], ignore_index=True)

    current_data_df.sort_values(
        by=['date'],
        ascending=True,
        inplace=True
    )

    updated_forecast_df.sort_values(
        by=['date'],
        ascending=True,
        inplace=True
    )

    fig = go.Figure()

    fig.add_trace(go.Scatter(
        x=current_data_df['date'],
        y=current_data_df['close_price'],
        mode='lines+markers',
        name='Stock Close Price'
    ))

    fig.add_trace(go.Scatter(
        x=updated_forecast_df['date'],
        y=updated_forecast_df['close_price'],
        mode='lines+markers',
        name='Forecast Close Price',
        line=dict(dash='dash')
    ))

    fig.update_layout(
        title="Close Price Forecasts",
        xaxis_title="Date",
        yaxis_title="Price (USD)",
        template="plotly"
    )

    st.plotly_chart(fig)





data = get_stocks()
stock_list = []
for stock_name in data:
    stock_list.append(stock_name)

stock_name = st.selectbox('Select a stock', stock_list)

data = get_relevant_data(stock_name)

plot_data(*data)
