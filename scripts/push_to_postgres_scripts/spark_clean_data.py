import logging
import pandas as pd
from scripts.push_to_postgres_scripts.get_last_date import get_last_date
from pyspark.sql.functions import lit, col
from datetime import date
from pyspark.sql import SparkSession
import sys

spark = SparkSession.builder \
    .appName("spark_clean_data") \
    .master("spark://spark-master:7077") \
    .config("spark.executor.memory", "512m") \
    .config("spark.driver.memory", "512m") \
    .config("spark.sql.shuffle.partitions", "2") \
    .getOrCreate()

mapping = {
    '1. open':'stocks_open',
    '2. high':'stocks_high',
    '3. low':'stocks_low',
    '4. close':'stocks_close',
    '5. adjusted close':'stocks_adjusted_close',
    '6. volume':'stocks_volume',
    '7. dividend amount':'stocks_dividend_amount',
    '8. split coefficient':'stocks_split_coefficient',
}

column_order = [
    'stocks_name',
    'stocks_date',
    'stocks_timezone',
    'stocks_open',
    'stocks_high',
    'stocks_low',
    'stocks_close',
    'stocks_adjusted_close',
    'stocks_volume',
    'stocks_dividend_amount',
    'stocks_split_coefficient'
]

logging.basicConfig(level=logging.INFO)

def spark_clean_data(**kwargs):

    previous_data = kwargs['ti'].xcom_pull(task_ids='clean_data', dag_id=kwargs['dag'].dag_id)

    if previous_data:
        return previous_data
    
    json_objects = kwargs['ti'].xcom_pull(task_ids='get_api_data', key='response_json')
    
    list_of_dataframes = []

    for json_object in json_objects:
        stock_name = json_object['Meta Data']['2. Symbol']
        time_series_data = json_object['Time Series (Daily)']

        values = time_series_data.values()

        time_zones_dataframes = []
        combined_df = None
        for time_zone, data in time_series_data.items():
            df = pd.DataFrame(data, index=[0])

            spark_df = spark.createDataFrame(df)
                
            spark_df = spark_df \
                .withColumn("stocks_timezone", lit(time_zone)) \
                .withColumn("stocks_name", lit(stock_name))

            spark_df = spark_df.withColumn("stocks_timezone", col("stocks_timezone").cast("timestamp"))

            if not combined_df:
                combined_df = spark_df
            else:
                combined_df = combined_df.union(spark_df)
        
        list_of_dataframes.append(combined_df)

    df = list_of_dataframes[0]
    for sdf in list_of_dataframes[1:]:
        df = df.union(sdf)
    
    df = df.dropDuplicates()
    df = df.dropna(thresh=3)
    for old_name, new_name in mapping.items():
        df = df.withColumnRenamed(old_name, new_name)
        try:
            df = df.withColumn(new_name, data_df[new_name].cast(IntegerType()))
        except:
            raise KeyError(f'Mapping to numeric values has failed. Column {value} does not exist in the dataframe')
    
    df = df.select(*column_order)

    kwargs['ti'].xcom_push(key='df', value=df)
