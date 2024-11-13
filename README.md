## Summary
For this project I created an ML pipeline to forecast the closing prices of stocks. This forecasting pipeline is scheduled to run daily using cron. Specifically, at 12am the latest stock data is gathered, cleaned, and pushed to the database. At 2 am a model training workflow is run which forecasts the stock closing price over the next 60 days.

- [![Python](https://img.shields.io/badge/Python-3.10-blue)](https://www.python.org)
- [![Pandas](https://img.shields.io/badge/Pandas-Data%20Analysis-green)](https://pandas.pydata.org)
- [![PySpark](https://img.shields.io/badge/PySpark-Big%20Data%20Analytics-yellow)](https://spark.apache.org/docs/latest/api/python/)
- [![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-Workflow%20Management-brightgreen)](https://airflow.apache.org)
- [![Docker](https://img.shields.io/badge/Docker-Containerization-blue)](https://www.docker.com)
- [![PostgreSQL](https://img.shields.io/badge/PostgreSQL-Database-blue)](https://www.postgresql.org)
- [![pgAdmin](https://img.shields.io/badge/pgAdmin-PostgreSQL%20Admin-orange)](https://www.pgadmin.org)
- [![Streamlit](https://img.shields.io/badge/Streamlit-Data%20Apps-red)](https://streamlit.io)


My main focus was on building the pipeline rather than the accuracy of the forecast. To create the ML pipeline I used 3 tools: postgres, airflow, and docker and to visualize the results I used streamlit.

![alt text](./source_images/finance_pipeline.png)

**Postgres**

Rather than using a csv to store the data from the api requests, I used a postgres database. Given the small size of the data, postgres is a bit overkill. However, I thought it would be useful to know how to set up a database from scratch.  

To assist me in the visualization of the database I used pgadmin4. 

**Airflow**

In short, airflow is a platform used to automate workflows.

Similarly to postgres, using airflow is a bit overkill because my workflows were not very complicated. I used airflow to create 3 workflows:
* A workflow to delete the current database (this was mostly used in testing).
* A workflow to get the api data and push it to the postgres database.
* A workflow to create forecasts.

I have attached images of the push_to_postgres and forecast workflow below.

![alt text](./source_images/push_to_postgres_workflow.png)
![alt text](./source_images/forecast_workflow.png)

**Docker**

To simplify the process of setting up a database and to streamline the pipeline creation process I used docker. 

**Streamlit**

Finally, to visualize the results, I created a container to run a streamlit script to display the closing price forecasts.

**PySpark**

For this project I wanted to try to use pyspark in the api data cleaning step. I was successful in installing pyspark however I've realised pyspark does not perform well for smaller datasets. Consequently, I still use the inital scripts which resorts to using pandas.

**Model**

Since the goal of this project was to familiarise myself with this data engineering tools rather than creating the most accurate model, I chose to use a Prophet model.

As hypothesized, the model's forecasts were unimpressive.
![Alt text](./source_images/streamlit_example.png)

**Next Steps**
* Improve the accruracy of the model.
* Incorporate monthly forecasts.
* Add pre-commits.
* Investigate weird forecasts for some firms.
