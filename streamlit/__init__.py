import streamlit as st
import argparse

# Use argparse to handle command line arguments
def parse_args():
    parser = argparse.ArgumentParser(description='Streamlit App with Arguments')
    parser.add_argument('--title', type=str, default='Default Dashboard', help='Title of the dashboard')
    parser.add_argument('--data_size', type=int, default=100, help='Size of the random dataset')
    args = parser.parse_args()
    return args

# Main function
def main():
    args = parse_args()

    # Use the arguments in your Streamlit app
    st.title(args.title)

    # Generate some data
    import pandas as pd
    import numpy as np
    data = pd.DataFrame(
        np.random.randn(args.data_size, 3),
        columns=['Column 1', 'Column 2', 'Column 3']
    )

    # Show data in the app
    st.dataframe(data)

if __name__ == '__main__':
    main()