import streamlit as st
import pandas as pd
import numpy as np

st.title('My First Streamlit App')

st.write("Hello, welcome to my first Streamlit app!")

slider_value = st.slider('Select a number', 0, 100, 50)

st.write(f'You selected: {slider_value}')

data = pd.DataFrame(
    np.random.randn(100, 3),
    columns=['A', 'B', 'C']
)

st.write("Here is some random data:")
st.dataframe(data)

st.line_chart(data)