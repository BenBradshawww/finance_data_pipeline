import streamlit as st
import pandas as pd
import numpy as np

#def get_stocks():


#def get_data():




stock = st.selectbox('Select a stock', [])

st.write(f'You selected: {slider_value}')

data = pd.DataFrame(
    np.random.randn(100, 3),
    columns=['A', 'B', 'C']
)

st.write("Here is some random data:")
st.dataframe(data)

st.line_chart(data)